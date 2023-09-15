import json
import subprocess
import sys

def execute_call(command, quiet=True, quiet_err=False, exit_script=True):
    kwargs = {"shell": True}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
    if quiet_err:
        kwargs["stderr"] = subprocess.DEVNULL
    code = subprocess.call(command, **kwargs)
    if code != 0 and exit_script:
        sys.exit(int(code))
    return code


def execute_output(command, quiet=False, quiet_err=False, output_json=True):
    kwargs = {"shell": True}
    if quiet:
        kwargs["stdout"] = subprocess.DEVNULL
    if quiet_err:
        kwargs["stderr"] = subprocess.DEVNULL
    output = subprocess.check_output(command, **kwargs)

    if not output_json:
        return output

    try:
        output = json.loads(output)
    except json.decoder.JSONDecodeError:
        pass
    return output
