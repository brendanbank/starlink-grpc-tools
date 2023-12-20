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

from datetime import datetime
import logging
import os
import signal
import sys
import time

import dish_common
import starlink_grpc
import queue


from prometheus_client import (Enum, Histogram, ProcessCollector, CollectorRegistry,
                               start_http_server, Gauge, Info,
                               generate_latest)
from prometheus_client.metrics_core import ( GaugeMetricFamily, InfoMetricFamily)


log = logging.getLogger(__name__)

DEFAULT_PORT = 9148

metrics_queue = queue.Queue()


CONNECTION_STATES = ["UNKNOWN", "CONNECTED", "BOOTING", "SEARCHING", "STOWED",
    "THERMAL_SHUTDOWN", "SLEEPING", "NO_SATS", "OBSTRUCTED", "NO_DOWNLINK", "NO_PINGS"]
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
    # parser = dish_common.create_arg_parser(
    #     output_description="print it in text format; by default, will print in CSV format")

    parser = dish_common.create_arg_parser(output_description="Prometheus exporter",
                                           bulk_history=False)

    group = parser.add_argument_group(title="Prometheus Exporter Options")

    # group.add_argument("-k",
    #                    "--skip-query",
    #                    action="store_true",
    #                    help="Skip querying for prior sample write point in history modes")

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


def loop_body(opts, gstate, metrics, registry, shutdown=False):
    metrics_data = {}
    starlink_id = None

    log.debug(f'loop_body started')

    def iform(val):
        if val is None:
            return 0
        elif( val is True):
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
    
    
    time_metrics =  {"rc": rc, "status_ts": status_ts, "hist_ts": hist_ts}
    
    # log.debug(f'metrics_data {metrics_data}')
    
    if metrics_data and 'id' in metrics_data:
        log.debug (f'starlink_id = {metrics_data["id"]["value"]}')
        
        starlink_id = metrics_data["id"]["value"]
        
        # set_metrics (starlink_id, metrics_data, metrics, time_metrics, registry)
        metrics_queue.put({"metrics_data": metrics_data, "time_metrics": time_metrics})
        
    return rc


class StarlinkCollector(object):
    def __init__(self):
        pass
        
    def collect(self):
        log.debug(f'collector called')
        
        metrics = self.set_metrics_family()
        
        for metric in metrics.keys():            
            yield (metrics[metric])

        
    def set_metrics_family (self):
    
        # self.test['info'] = GaugeMetricFamily ('test_value', 'test information', labels=['id'])
        return_metrics = {}
        
        while not metrics_queue.empty():
            metrics = metrics_queue.get()
            
            metrics_data = metrics['metrics_data']
            time_metrics = metrics['time_metrics']
            id = metrics_data['id']['value']
            info_metrics = {}

            log.debug(f'id {id}')
        
            # create info and state

            # if not 'state' in metrics:
            #     metrics['state'] = GaugeMetricFamily(f'{STARLINK_NAME}_status', 'Starlink Status', labels=['id'])
                
            for metric in metrics_data.keys():
                # log.debug(f'metric {metric}')
                if type(metrics_data[metric]['value']) == str and  metric != 'id':
                    info_metrics[metric] = metrics_data[metric]['value']
                    
                if type(metrics_data[metric]['value']) == float or type(metrics_data[metric]['value']) == int:
                    if not metric in return_metrics:
                        return_metrics[metric] = GaugeMetricFamily(name=f'{STARLINK_NAME}_{metric}',
                                                        documentation=metrics_data[metric]['text'],
                                                        labels=['id'])
                    
                    return_metrics[metric].add_metric(labels=[id],
                                        value=metrics_data[metric]['value'],
                                        timestamp=time_metrics['status_ts'])
        
                    if not 'info' in return_metrics:
                        return_metrics['info'] = InfoMetricFamily(f'{STARLINK_NAME}', 'Starlink Info', labels=['id'])

            
            return_metrics['info'].add_metric(labels=[id], value=info_metrics, timestamp=time_metrics['status_ts'])
            
            if not 'state' in return_metrics:
                return_metrics['state'] = GaugeMetricFamily(name=f'{STARLINK_NAME}_status',
                                                documentation=metrics_data['state']['text'],
                                                labels=['id', 'starlink_status'])
                    

            log.debug (f"state: {metrics_data['state']['value']}")                    
                
            [ return_metrics['state'].add_metric(labels=[id, s],
                                value=1 if metrics_data['state']['value'] == s else 0,
                                timestamp=time_metrics['status_ts']) 
                                for i, s
                                in enumerate(CONNECTION_STATES) ]

            
            
            log.debug(f'status_ts = {time_metrics["status_ts"]}')
            
            
        return(return_metrics)
    

def main():
    opts = parse_args()
    
    opts.numeric = True
    metrics = {}
        
    logging.basicConfig(format="%(levelname)s: %(message)s")

    gstate = dish_common.GlobalState(target=opts.target)
    
    signal.signal(signal.SIGTERM, handle_sigterm)
    
    if opts.debug:
        log.setLevel(level=logging.DEBUG)
        log.debug('debugging enabled')
    
    rc = 0
    
    registry = CollectorRegistry()
    registry.register(StarlinkCollector())

    start_http_server(opts.exporter_port, registry=registry)
    
    
    try:
        next_loop = time.monotonic()
        while True:
            rc = loop_body(opts, gstate, metrics, registry)

            if opts.loop_interval > 0.0:
                now = time.monotonic()
                next_loop = max(next_loop + opts.loop_interval, now)
                time.sleep(next_loop - now)
            else:
                break
    except (KeyboardInterrupt, Terminated):
        pass
    finally:
        loop_body(opts, gstate, metrics, registry, shutdown=True)
        gstate.shutdown()    
    
    sys.exit(rc)


if __name__ == "__main__":
    main()
