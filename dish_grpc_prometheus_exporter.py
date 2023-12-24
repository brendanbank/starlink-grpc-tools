#!/usr/bin/python3
"""Output Starlink user terminal data info in text format.

This script pulls the current status info and/or metrics computed from the
history data and prints them to a file or stdout either once or in a periodic
loop. By default, it will print the results in CSV format.

Note that using this script to record the alert_detail group mode as CSV
data is not recommended, because the number of alerts and their relative
order in the output can change with the dish software. Instead of using
the alert_detail mode, you can use the alerts bitmask in the status group.
"""

import logging
import signal
import sys
import time

import dish_common
import queue

from prometheus_client import (CollectorRegistry, start_http_server)
from prometheus_client.metrics_core import (GaugeMetricFamily, InfoMetricFamily)

log = logging.getLogger(__name__)

DEFAULT_PORT = 9148

CONNECTION_STATES = ["UNKNOWN", "CONNECTED", "BOOTING", "SEARCHING",
                     "STOWED", "THERMAL_SHUTDOWN", "SLEEPING", "NO_SATS",
                     "OBSTRUCTED", "NO_DOWNLINK", "NO_PINGS", "DISH_UNREACHABLE"]


STARLINK_NAME = "starlink"
COUNTER_FIELD = "end_counter"
VERBOSE_FIELD_MAP = {
    # status fields (the remainder are either self-explanatory or I don't
    # know with confidence what they mean)
    "alerts": "Alerts bit field",

    # ping_drop fields
    "samples": "Parsed samples",
    "end_counter": "Sample counter",
    "total_ping_drop": "Total ping drop",
    "count_full_ping_drop": "Count of drop == 1",
    "count_obstructed": "Obstructed",
    "total_obstructed_ping_drop": "Obstructed ping drop",
    "count_full_obstructed_ping_drop": "Obstructed drop == 1",
    "count_unscheduled": "Unscheduled",
    "total_unscheduled_ping_drop": "Unscheduled ping drop",
    "count_full_unscheduled_ping_drop": "Unscheduled drop == 1",

    # ping_run_length fields
    "init_run_fragment": "Initial drop run fragment",
    "final_run_fragment": "Final drop run fragment",
    "run_seconds": "Per-second drop runs",
    "run_minutes": "Per-minute drop runs",

    # ping_latency fields
    "mean_all_ping_latency": "Mean RTT, drop < 1",
    "deciles_all_ping_latency": "RTT deciles, drop < 1",
    "mean_full_ping_latency": "Mean RTT, drop == 0",
    "deciles_full_ping_latency": "RTT deciles, drop == 0",
    "stdev_full_ping_latency": "RTT standard deviation, drop == 0",

    # ping_loaded_latency is still experimental, so leave those unexplained

    # usage fields
    "download_usage": "Bytes downloaded",
    "upload_usage": "Bytes uploaded",
}


class Terminated(Exception):
    pass


def handle_sigterm(signum, frame):
    # Turn SIGTERM into an exception so main loop can clean up
    raise Terminated


def parse_args():

    parser = dish_common.create_arg_parser(output_description="Prometheus exporter",
                                           bulk_history=False)

    group = parser.add_argument_group(title="Prometheus Exporter Options")

    group.add_argument("-d",
                       "--debug",
                       action="store_true",
                       help="print debugging information to stderr.")
    
    group.add_argument("-p",
                       "--exporter-port",
                       type=int,
                       default=int(DEFAULT_PORT),
                       help="Exporter Port : " + 
                       str(DEFAULT_PORT))
    
    opts = dish_common.run_arg_parser(parser, modes=['status', 'obstruction_detail', 'alert_detail', 'location', 'ping_drop', 'usage'])

    if (opts.history_stats_mode or opts.status_mode) and opts.bulk_mode and not opts.verbose:
        parser.error("bulk_history cannot be combined with other modes for CSV output")

    return opts

class StarlinkCollector(object):

    def __init__(self):
        self.last_dish_id = 'Unknown'
        self.metrics_queue = queue.Queue()
        pass
        
    def collect(self):
        log.debug(f'collector called')
        
        metrics = self.set_metrics()
        
        for metric in metrics.keys(): 
            yield (metrics[metric])
        
    def set_metrics (self):
    
        return_metrics = {}
        
        while not self.metrics_queue.empty():
            metrics = self.metrics_queue.get()
            
            metrics_data = metrics['metrics_data']
            time_metrics = metrics['time_metrics']
            dish_id = metrics_data['id']['value']
            info_metrics = {}

            log.debug(f'id {dish_id}')
                        
            for metric in metrics_data.keys():
                
                if "snr" == metric:
                    # snr is not supported by starlink any more but still returned by the grpc
                    # service for backwards compatibility
                    continue

                log.debug(f'metric {metric} metrics_data = {metrics_data[metric]}')
                if type(metrics_data[metric]['value']) == str and  metric != 'id' and metric != 'state' :
                    info_metrics[metric] = metrics_data[metric]['value']
                    continue
                    
                if type(metrics_data[metric]['value']) == float or type(metrics_data[metric]['value']) == int:
                    if not metric in return_metrics:
                        return_metrics[metric] = GaugeMetricFamily(name=f'{STARLINK_NAME}_{metric}',
                                                        documentation=metrics_data[metric]['text'],
                                                        labels=['id'])
                    
                    return_metrics[metric].add_metric(labels=[dish_id],
                                        value=metrics_data[metric]['value'],
                                        timestamp=time_metrics)
        
            if not 'info' in return_metrics:
                return_metrics['info'] = InfoMetricFamily(f'{STARLINK_NAME}', 'Starlink Info', labels=['id'])
            
            return_metrics['info'].add_metric(labels=[dish_id], value=info_metrics, timestamp=time_metrics)
            
            if 'state' in metrics_data:
                self._add_status(return_metrics,dish_id, metrics_data['state']['value'] , time_metrics)
                        
        return(return_metrics)
    
    def _add_status(self,return_metrics, dish_id, starlink_status,time_metrics ):
            starlink_states = { i:s for i, s in enumerate(CONNECTION_STATES) }
            docu = ""
            for key, val in starlink_states.items():
                docu = docu + f"{key} = {val}, " 

            if not 'state' in return_metrics:
                return_metrics['state'] = GaugeMetricFamily(name=f'{STARLINK_NAME}_dish_status',
                                                documentation="Starlink Status " + docu,
                                                labels=['id'])
                
            for key,value in starlink_states.items():
                if starlink_status == value:
                    return_metrics['state'].add_metric(labels=[dish_id], value=key,
                                                       timestamp=time_metrics)

    def loop_body(self, opts, gstate, shutdown=False):
        metrics_data = {}
    
        log.debug(f'loop_body started')
    
        def iform(val):
            if val is None:
                return 0
            elif(val is True):
                return 1
            elif(val is False):
                return 0
            else:
                return (val)
    
        def cb_data_add_item(name, val, category):
            metrics_data[name] = {'value': iform(val),
                        'text': VERBOSE_FIELD_MAP.get(name, name),
                        'category': category }
                
        def cb_data_add_sequence(name, val, category, start):
            pass
    
        def cb_add_bulk(bulk, count, timestamp, counter):
            pass
        
        rc, status_ts, hist_ts = dish_common.get_data(opts,
                                                      gstate,
                                                      cb_data_add_item,
                                                      cb_data_add_sequence,
                                                      add_bulk=cb_add_bulk,
                                                      flush_history=shutdown)
        
        log.debug(f'retun code: rc {rc} ')
        if (status_ts is None or hist_ts is None):
            log.debug(f'status_ts {status_ts} hist_ts {hist_ts}')
        
        
        # log.debug(f'metrics_data {metrics_data}')
        
        if rc == 0 and metrics_data and 'id' in metrics_data:
            self.last_dish_id = metrics_data["id"]["value"]
            log.debug (f'starlink_id = {metrics_data["id"]["value"]}')
            self.metrics_queue.put({"metrics_data": metrics_data, "time_metrics": status_ts})
        else:
            metrics_data['state'] = {'value': 'NO_CONNECTION_WITH_DISH',
                        'text': 'state',
                        'category': 'status' }
            
            metrics_data['id'] = {'value': self.last_dish_id,
                        'text': VERBOSE_FIELD_MAP.get(id, id),
                        'category': 'status' }
            time_metrics = int(time.time())
            self.metrics_queue.put({"metrics_data": metrics_data, "time_metrics": time_metrics})

        return rc
    
       
def main():
    opts = parse_args()
    
    opts.numeric = True
        
    logging.basicConfig(format="%(levelname)s: %(message)s")

    gstate = dish_common.GlobalState(target=opts.target)
    
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    if opts.debug:
        log.setLevel(level=logging.DEBUG)
        log.debug('debugging enabled')
    
    rc = 0
    
    registry = CollectorRegistry()
    
    collector = StarlinkCollector()
    registry.register(collector)

    start_http_server(opts.exporter_port, addr='::', registry=registry)


    try:
        next_loop = time.monotonic()
        while True:
            log.debug('run loop_body')
            rc = collector.loop_body(opts, gstate)
            if opts.loop_interval > 0.0:
                now = time.monotonic()
                next_loop = max(next_loop + opts.loop_interval, now)
                time.sleep(next_loop - now)
            else:
                break
    except (KeyboardInterrupt, Terminated):
        pass
    finally:
        collector.loop_body(opts, gstate, shutdown=True)
        gstate.shutdown()    
    
    sys.exit(rc)


if __name__ == "__main__":
    main()
