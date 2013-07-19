import logging
from argparse import ArgumentParser
from splice.cluster.instance import run_instance

if __name__ == "__main__":
    parser = ArgumentParser()
    parser.add_argument("-m", "--master-address",
                        help="register with the specified MASTER_ADDRESS",
                        metavar="MASTER_ADDRESS",
                        dest="master_address")
    parser.add_argument("-t", "--keepalive-timeout",
                        help="TIMEOUT number of seconds before master node is considered offline",
                        metavar="KEEPALIVE_TIMEOUT",
                        dest="keepalive_timeout",
                        default=120,
                        type=int)

    parser.add_argument("-l", "--log-level", help="Log LEVEL to use", metavar="LEVEL", dest="log_level",
                        default=logging.WARN)

    options = parser.parse_args()

    run_instance(options.master_address, log_level=options.log_level, keepalive_timeout=options.keepalive_timeout)