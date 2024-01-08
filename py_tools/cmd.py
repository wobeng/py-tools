import json
import subprocess
import sys
import logging

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def execute_call(command, quiet=True, quiet_err=False, exit_script=True, log_command=False):
    kwargs = {"shell": True}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
    if quiet_err:
        kwargs["stderr"] = subprocess.DEVNULL
    if log_command:
        logger.info("command: %s" % command)

    code = subprocess.call(command, **kwargs)

    if code != 0 and exit_script:
        logger.info("command: %s" % command)
        sys.exit(int(code))

    return code


def execute_output(command, quiet=False, quiet_err=False, output_json=True, log_command=False):
    kwargs = {"shell": True}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
    if quiet_err:
        kwargs["stderr"] = subprocess.DEVNULL
    if log_command:
        logger.info("command: %s" % command)

    try:
        output = subprocess.check_output(command, **kwargs)
        # Explicitly decode using UTF-8
        output = output.decode('utf-8').rstrip()
    except subprocess.CalledProcessError as e:
        logger.info("error executing command: %s" % e)
        logger.info("command output: %s" % e.output)
        raise

    if not output_json:
        return output

    try:
        output = json.loads(output)
    except json.decoder.JSONDecodeError:
        pass

    return output
