"""
Convert a directory into an executable
"""
from relay_mesos.main import main, build_arg_parser


def go():
    NS = build_arg_parser().parse_args()
    main(NS)

if __name__ == '__main__':
    go()
