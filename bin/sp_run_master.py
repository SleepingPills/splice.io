import logging
from argparse import ArgumentParser
from splice.cluster.master import run_master

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-t", "--node-timeout", help="node ping TIMEOUT", metavar="TIMEOUT", dest="timeout", default=60,
                        type=int)
    parser.add_argument("-p", "--port", help="PORT to listen on", metavar="PORT", dest="port", default=58950, type=int)
    parser.add_argument("-l", "--log-level", help="Log LEVEL to use", metavar="LEVEL", dest="log_level",
                        default=logging.WARN)

    options = parser.parse_args()

    node_timeout = options.timeout
    port = options.port

    run_master(node_timeout=node_timeout, port=port, log_level=options.log_level)