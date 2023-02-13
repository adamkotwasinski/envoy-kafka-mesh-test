#!/usr/bin/env python3

import argparse
import json
import logging
import re
import requests
import sys

from os import listdir
from os.path import isfile, join

# Point this script at a directory containing two repos with Kafka sources, with different versions.
# Basically a glorified diff.

KAFKA_DATA_PATH='clients/src/main/resources/common/message'

logging.basicConfig(stream = sys.stdout,
                    format = '%(message)s',
                    level = logging.INFO)
logger = logging.getLogger('compare')


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument('-r')
    parser.add_argument('-v1')
    parser.add_argument('-v2')
    args = parser.parse_args(sys.argv[1:])
    if args.r is None or args.v1 is None or args.v2 is None:
        logger.info("Missing arguments: -r, -v1, -v2")
        exit(1)
    else:
        run(args.r, args.v1, args.v2)


def run(root: str, kafka_version_1: str, kafka_version_2: str):
    path1 = '%s/%s/%s' % (root, kafka_version_1, KAFKA_DATA_PATH)
    path2 = '%s/%s/%s' % (root, kafka_version_2, KAFKA_DATA_PATH)
    logger.info('Comparing [%s] with [%s]', path1, path2)

    # Response versions always match request versions so we only need to identify new request types.
    files1 = [join(path1, f) for f in listdir(path1) if isfile(join(path1, f)) and f.endswith('Request.json')]
    data1 = parse(files1)

    files2 = [join(path2, f) for f in listdir(path2) if isfile(join(path2, f)) and f.endswith('Request.json')]
    data2 = parse(files2)

    outdated = [x for x in data1.keys() if x not in data2.keys()]
    if outdated:
        # This might need manual intervention - there's some API key that's no longer supported.
        # But this should never happen as servers need to be backwards compatible.
        raise ValueError('api key removed: ' + str(outdated))

    keys = sorted(data2.keys())
    for k in keys:
        if k in data1.keys():
            # Check request versions.
            v1 = data1[k][1]
            v2 = data2[k][1]
            logger.debug("%s -> %s vs %s", k, v1, v2)
            lastSupported1 = v1[-1]
            lastSupported2 = v2[-1]
            if lastSupported1 < lastSupported2:
                # New request version (what we wanted to find).
                logger.info("%s -> New versions in %s: new %s vs old %s", k, data2[k][0], lastSupported2, lastSupported1)
            elif lastSupported1 == lastSupported2:
                # No changes.
                pass
            else:
                # Bad data?
                logger.warn("%s -> older version supports higher version than new: %s / %s", k, lastSupported1, lastSupported2)
                pass
        else:
            # New request type.
            logger.info("%s -> New request type: %s", k, data2[k][0])


# From Envoy - https://github.com/envoyproxy/envoy/blob/v1.25.1/contrib/kafka/filters/network/source/protocol/generator.py
def parse(files):
    result = {}
    for input_file in files:
        try:
            with open(input_file, 'r') as fd:
                raw_contents = fd.read()
                without_comments = re.sub(r'\s*//.*\n', '\n', raw_contents)
                without_empty_newlines = re.sub(
                    r'^\s*$', '', without_comments, flags=re.MULTILINE)
                message_spec = json.loads(without_empty_newlines)

                message_type = message_spec['name']
                api_key = message_spec['apiKey']
                versions = parse_version_string(message_spec['validVersions'], 2 << 16 - 1)

                result[api_key] = (message_type, versions)

                #logger.info('%s %s %s', message_type, api_key, versions)
        except Exception as e:
            print('could not process %s' % input_file)
            raise
    return result


# From Envoy - https://github.com/envoyproxy/envoy/blob/v1.25.1/contrib/kafka/filters/network/source/protocol/generator.py
def parse_version_string(raw_versions, highest_possible_version):
    if raw_versions.endswith('+'):
        return range(int(raw_versions[:-1]), highest_possible_version + 1)
    else:
        if '-' in raw_versions:
            tokens = raw_versions.split('-', 1)
            return range(int(tokens[0]), int(tokens[1]) + 1)
        else:
            single_version = int(raw_versions)
            return range(single_version, single_version + 1)


if __name__ == "__main__":
    main()
