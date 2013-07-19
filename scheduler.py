#!/usr/bin/env python

import sys
import traceback
import os
import re
import argparse
import multiprocessing
import subprocess
from datetime import datetime


def print_p(string, color=None):
    """
    print text on console

    :color: (black|yellow|cyan|blue|red|white|grey|purple)
    """
    colors = {"black": "0;30", "yellow": "1;33",
              "cyan": "0;36", "blue": "0;34",
              "red": "0;31", "white": "1;37",
              "grey": "0;37", "purple": "0;35"}
    n = datetime.now()
    ccode = ""
    if color is not None and color in colors:
        ccode = "\033[%sm" % colors[color]
    print "%s - %s%s\033[0m" % (n.strftime("%Y-%m-%d %H:%M:%S"), ccode, string)


def run_functor(functor, *args):
    """
    Given a no-argument functor, run it and return its result. We can
    use this with multiprocessing.map and map it over a list of job
    functors to do them.

    Handles getting more than multiprocessing's pitiful exception output
    """
    import traceback
    try:
        return functor(*args)
    except:
        raise Exception("".join(traceback.format_exception(*sys.exc_info())))


def run_command(cmdstr, sleeptime=0):
    cmd_print = cmdstr.replace("\n", "")
    os.system("sleep %d" % sleeptime)
    print_p("Running \"%s\"" % cmd_print, "yellow")
    ret = os.system(cmdstr)
    print_p("Command \"%s\" finished with status %d" % (cmd_print, ret), "yellow")


def main():
    parser = argparse.ArgumentParser(description="start parallel jobs")
    parser.add_argument('-n', '--cores', nargs=1, type=int,
                        required=False, help="set maximum number of \
                        CPU cores to use")
    parser.add_argument("input_file", help="A file containing the shell commands \
                         to execute (one per line)")
    parser.add_argument("-s", "--simulate", required=False, action="store_true",
                        help="Only print commands, don't run them")
    parser.add_argument("-e", "--execute", required=False, action="store_true",
                        help="instead of reading the input file, execute it and take \
                              its output as commands")
    parser.add_argument("-d", "--delay", required=False, type=int,
                        default=0, help="add a delay before each command.")
    args = parser.parse_args()

    execute = args.execute
    simulate = args.simulate
    input_file = args.input_file
    delay = args.delay
    num_cpus = multiprocessing.cpu_count()
    process_limit = args.cores[0] if args.cores is not None else num_cpus
    process_limit = min(process_limit, num_cpus)
    print_p("Using %d CPUs" % process_limit, "yellow")

    pool = multiprocessing.Pool(process_limit)
    result_set = []

    if not execute:
        with open(input_file, "r") as f:
            cmds = [l for l in f]
    else:
        ostr = str(subprocess.check_output(input_file))
        cmds = ostr.split("\n")

    i = 0
    for c in cmds:
        if simulate:
            print_p("NOT running command \"%s\"" % c.replace("\n", ""), "yellow")
        else:
            result_set.append(pool.apply_async(run_functor,
                                               [run_command, c, i * delay]))
        i += 1

    pool.close()
    pool.join()
    for r in result_set:
        r.get() # raise exceptions if any

    print_p("All jobs are finished", "cyan")


if __name__ == '__main__':
    main()
