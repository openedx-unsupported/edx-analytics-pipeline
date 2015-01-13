import argparse
import ConfigParser
import sys


def main(override_cfg, base_cfg):
    """
    This function will read in the configs in override_cfg and base_cfg (filenames), and then
    for every item in every section of override_cfg, override the corresponding config in base_cfg
    if it exists, or else create the config if it doesn't exist.
    The output is written to stdout
    """
    override_parser = ConfigParser.ConfigParser()
    override_parser.read(override_cfg)

    base_parser = ConfigParser.ConfigParser()
    base_parser.read(base_cfg)

    for section in override_parser.sections():
        for option, value in override_parser.items(section):
            base_parser.set(section, option, value)

    base_parser.write(sys.stdout)


if __name__ == "__main__":
    arg_parser = argparse.ArgumentParser(
        description='Override a base .cfg file with an override .cfg file , outputs to stdout',
    )
    arg_parser.add_argument('override_cfg_file', type=str, help=".cfg file that has the overrides")
    arg_parser.add_argument('base_cfg_file', type=str, help="base .cfg file whose settings would get overridden")
    args = arg_parser.parse_args()
    main(args.override_cfg_file, args.base_cfg_file)
