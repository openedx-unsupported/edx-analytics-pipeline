edx-analytics-pipeline
===============
The Hadoop-based data pipeline.

Installation instructions
--------------------

We have some draft docs: 

* https://openedx.atlassian.net/wiki/display/OpenOPS/edX+Analytics+Installation describes how to install a small scale analytics stack on a single machine. It'll get merged into the next doc below at some point. Help welcome!
* http://edx.readthedocs.org/projects/edx-installing-configuring-and-running/en/latest/analytics/install_analytics.html has an overview of a larger scale AWS install.


Requirements
------------
Your machine will need the following in order to run the code in this repository:

* [Python](https://www.python.org/) 2.7.x
* [GCC](http://gcc.gnu.org/) (to compile numpy)
* [MySQL](http://mysql.com)
* [GnuPG](https://www.gnupg.org/) 1.4.x

All of the components above can be installed with your preferred package manager (e.g. apt, yum, [brew](http://brew.sh).

The requirements in requirements/default.txt and requirements/test.txt can be installed with pip (via make):

    make requirements

*Known Issues on Mac OS X*

If you are running the code on Mac OS X, you may encounter a couple issues when installing [numpy](https://pypi.python.org/pypi/numpy).
If pip complains about being unable to compile Fortran, ensure that you have GCC installed. The easiest way to install GCC is using
[Homebrew](http://brew.sh/): `brew install gcc`. If after installing GCC you see an error along the lines of `cannot link a simple C program`,
execute the following command to trigger the compiler to *not* throw an error when it encounters unused command arguments:

    export ARCHFLAGS=-Wno-error=unused-command-line-argument-hard-error-in-future

Note: If you need to frequently re-install/upgrade requirements, you may find it convenient to add the export statements above
to your .bashrc or .bash_profile file so that the statement is run whenever you open a new shell.

**Wheel**

[Wheel](http://wheel.readthedocs.org/en/latest/) can help cut down the time to install requirements. The Makefile is setup
to use the environment variables `WHEEL_URL` and `WHEEL_PYVER` to find the Wheel server. You can set these variables using the commands below.
If you want to set these variables every time you open a shell, add them to your .bashrc or .bash_profile files.


    export WHEEL_PYVER=2.7
    export WHEEL_URL=http://edx-wheelhouse.s3-website-us-east-1.amazonaws.com/<OPERATING SYSTEM>/<OS VARIANT>

Values for `<OPERATING SYSTEM>/<OS VARIANT>`:

* `Ubuntu/precise`
* `MacOSX/lion`


Running the Tests
-----------------
Run `make test` to install the Python requirements and run the unit tests.

Some of the tests rely on AWS. If you encounter errors such as `NoAuthHandlerFound: No handler was ready to authenticate. 1 handlers were checked. ['HmacAuthV1Handler'] Check your credentials`,
you need to set the `AWS_ACCESS_KEY_ID` and `AWS_SECRET_ACCESS_KEY` environment variables. The values do not need to be
valid credentials for the tests to pass, so the commands below should fix the failures.

    export AWS_ACCESS_KEY_ID='AK123'
    export AWS_SECRET_ACCESS_KEY='abc123'


How to Contribute
-----------------

Contributions are very welcome, but for legal reasons, you must submit a signed
[individual contributor's agreement](http://code.edx.org/individual-contributor-agreement.pdf)
before we can accept your contribution. See our
[CONTRIBUTING](https://github.com/edx/edx-platform/blob/master/CONTRIBUTING.rst)
file for more information -- it also contains guidelines for how to maintain
high code quality, which will make your contribution more likely to be accepted.
