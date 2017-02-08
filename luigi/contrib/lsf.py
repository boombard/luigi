import os
import inspect
import subprocess
import time
import sys
import logging
import random
try:
    import cPickle as pickle
except ImportError:
    import pickle
from datetime import datetime as dtdt

import luigi
from luigi.contrib import hadoop
from luigi.contrib import sge_runner

logger = logging.getLogger('luigi-interface')
logger.propagate = 0

POLL_TIME = 5


def _parse_bsub_job_id(output):
    return int(output.split("<")[1].split(">")[0])


class LSFJob(luigi.Task):

    n_cpu = luigi.IntParameter(default=2, significant=False)
    memory = luigi.IntParameter(default=None, significant=False)
    resource_flag = luigi.Parameter(default=None, significant=False)
    queue_flag = luigi.Parameter(default="normal", significant=False)
    shared_tmp_dir = luigi.Parameter(default=None, significant=False)
    log_dir = luigi.Parameter(default=None, significant=False)
    job_name = luigi.Parameter(
        significant=False,
        default=None,
        description="Explicit job name give via bsub")
    job_name_format = luigi.Parameter(
        significant=False,
        default=None,
        description="A string that can be formatted with class variables"
                    "to name the job with bsub")
    run_locally = luigi.BoolParameter(
        significant=False,
        default=False,
        description="Run locally instead of on the cluster")
    poll_time = luigi.Parameter(
        significant=False,
        default=POLL_TIME,
        description="The wait time to poll bjobs")
    dont_remove_tmp_dir = luigi.BoolParameter(
        significant=False,
        default=False,
        description="Don't delete the temporary directory used")
    extra_bsub_args = luigi.Parameter(
        significant=False,
        default=None)

    def __init__(self, *args, **kwargs):
        super(LSFJob, self).__init__(*args, **kwargs)
        if self.job_name:
            pass
        elif self.job_name_format:
            self.job_name = self.job_name_format.format(
                task_family=self.task_family, **self.__dict__)
        else:
            self.job_name = self.task_family

        if self.shared_tmp_dir is None:
            self.shared_tmp_dir = os.path.join(os.getcwd(), ".luigi")
        if not os.path.isdir(self.shared_tmp_dir):
            os.makedirs(self.shared_tmp_dir)

        if not self.log_dir:
            self.log_dir = self.shared_tmp_dir

        timestamp = dtdt.now().strftime("%y%m%d-%H%M%S")
        self.outfile = os.path.join(
            self.log_dir, 'job.{name}.{time}.out'.format(name=self.job_name,
                                                         time=timestamp))
        self.errfile = os.path.join(
            self.log_dir, 'job.{name}.{time}.err'.format(name=self.job_name,
                                                         time=timestamp))
        self.tmp_dir = os.getcwd()

    def _fetch_task_failures(self):
        if not os.path.isfile(self.errfile):
            logger.info("No Error file")
            return ""
        with open(self.errfile, "r") as f:
            errors = f.read()
        return errors

    def job_cmd(self):
        return ""

    def _clean(self):
        pass

    def _run_job(self):

        args = []

        args += ["bsub", "-q", self.queue_flag]
        args += ["-n", str(self.n_cpu)]
        if self.memory:
            args += ["-M %s" % self.memory]
            if self.resource_flag is None:
                args += [
                    "-R", '"rusage[mem={memory}] '
                          'select[mem>{memory}] '
                          'span[hosts=1]"'.format(memory=self.memory)]
        if self.resource_flag:
            args += ["-R", '"%s"' % self.resource_flag]
        args += ["-o", self.outfile]
        args += ["-e", self.errfile]
        if self.extra_bsub_args:
            args += self.extra_bsub_args.split()

        job_str = self.job_cmd()
        args += [job_str]

        logger.debug("LSF command: %s" % ' '.join(args))

        # output = subprocess.check_output(args, shell=True)

        p = subprocess.Popen(args, stdin=subprocess.PIPE,
                             stdout=subprocess.PIPE, cwd=self.tmp_dir)
        output = str(p.communicate()[0])
        logger.debug("Output: %s" % output)
        self.job_id = _parse_bsub_job_id(output)
        logger.debug("Submitted job to bsub with response:\n" + str(output))

        time.sleep(3)
        self._track_job()

        # Allow for file system delays
        time.sleep(5)

        self._clean()

    def _track_job(self):
        unknown_count = 0
        while True:

            time.sleep(self.poll_time)

            cmd = "bjobs %d | awk 'FNR==2 {print $3}'" % self.job_id
            p = subprocess.Popen(cmd, stdin=subprocess.PIPE,
                                 stdout=subprocess.PIPE,
                                 shell=True)
            lsf_status = p.communicate()[0].strip()
            if isinstance(lsf_status, bytes):
                lsf_status = lsf_status.decode('ascii')

            # See what the job's up to
            logger.info("LSF Status: %s" % lsf_status)
            if lsf_status == "RUN":
                logger.info("Job is running...")
            elif lsf_status == "PEND":
                logger.info("Job is pending...")
            elif lsf_status == "EXIT" or lsf_status == "DONE":
                error = self._fetch_task_failures()
                if lsf_status == "EXIT":
                    if not len(error):
                        logger.info("Job has exited unexpectedly without error")
                    else:
                        logger.error("Job has FAILED")
                        logger.error(error)
                    raise Exception("Job has FAILED")
                if lsf_status == "DONE":
                    if not len(error):
                        logger.info("Job has completed successfully")
                    else:
                        logger.info("Job has completed with errors")
                        logger.error(error)
                    break
            elif lsf_status == "SSUSP":
                logger.info("Job is suspended (basically, pending)...")
            elif lsf_status == "UNKWN":
                unknown_count += 1
                logger.info("Job status has been indeterminate for %d checks." % unknown_count)
                if unknown_count > 5:
                    logger.info("Job status is UNKWN. Moving on...")
                    break
            else:
                logger.info("Job status is UNKNOWN!")
                logger.info("Status is : %s" % lsf_status)
                raise Exception("Unknown status: %s" % lsf_status)


class LSFJobTask(LSFJob):

    no_tarball = luigi.BoolParameter(
        significant=False,
        default=False,
        description="Don't tarball (and extract) the luigi project files")

    def _init_local(self):

        base_tmp_dir = self.shared_tmp_dir
        random_id = "%016x" % random.getrandbits(64)

        folder_name = self.task_id + '-' + random_id
        self.tmp_dir = os.path.join(base_tmp_dir, folder_name)

        max_filename_length = os.fstatvfs(0).f_namemax
        self.tmp_dir = self.tmp_dir[:max_filename_length]

        logger.info("Tmp dir: %s", self.tmp_dir)
        os.makedirs(self.tmp_dir)

        logging.debug("Dumping pickled class")
        self._dump(self.tmp_dir)

        if not self.no_tarball:
            # Make sure that all the class's dependencies are tarred, available
            # This is not necessary if luigi is importable from the cluster node
            logging.debug("Tarballing dependencies")
            # Grab luigi and the module containing the code to be run
            packages = [luigi] + \
                       [__import__(self.__module__, None, None, 'dummy')]
            hadoop.create_packages_archive(
                packages, os.path.join(self.tmp_dir, "packages.tar"), log=False)

        self.init_local()

    def init_local(self):
        """ Implement any pre-work initialisations here """
        pass

    def run(self):
        if self.run_locally:
            self.work()
        else:
            self._init_local()
            self._run_job()
            # The procedure:
            # - Pickle the class
            # - Tarball the dependencies
            # - Construct a bsub argument that runs a generic runner function
            # - Runner function loads the class from pickle
            # - Runner class untars the dependencies
            # - Runner function hits the button on the class's work() method

    def work(self):
        """Override this method, rather than ``run()``"""
        pass

    def _dump(self, out_dir=''):
        """Dump instance to file."""
        with self.no_unpicklable_properties():
            self.job_file = os.path.join(out_dir, 'job-instance.pickle')
            logging.info("job file: " + self.job_file)
            if self.__module__ == '__main__':
                d = pickle.dumps(self)
                module_name = os.path.basename(sys.argv[0])
                logger.info("Module is main: %s" % module_name)
                logger.info("inspect module: %s" % inspect.getmodulename(sys.argv[0]))
                d = d.replace(b'(c__main__', b"(c" + module_name.encode('ascii'))
                open(self.job_file, "wb").write(d)
            else:
                logger.info("Module is not main")
                pickle.dump(self, open(self.job_file, "wb"), protocol=2)

    def job_cmd(self):
        runner_path = sge_runner.__file__
        if runner_path.endswith("pyc"):
            runner_path = runner_path[:-3] + "py"
        job_str = 'python {0} "{1}" "{2}"'.format(
            runner_path, self.tmp_dir, os.getcwd())
        if self.no_tarball:
            job_str += ' --no-tarball'

        if not job_str:

            # Find where our file is
            runner_path = sge_runner.__file__

            # assume source is next to compiled
            if runner_path.endswith("pyc"):
                runner_path = runner_path[:-3] + "py"

            job_str = " ".join([str(runner_path), str(self.tmp_dir)])

        return job_str

    def _clean(self):
        if (self.tmp_dir and os.path.exists(self.tmp_dir) and
                not self.dont_remove_tmp_dir):
            logger.info("Removing temporary directory %s" % self.tmp_dir)
            subprocess.call(["rm", "-rf", self.tmp_dir])


class LSFShellJob(LSFJob):

    def run(self):
        if self.run_locally:
            subprocess.check_output(self.job_cmd(), shell=True)
        else:
            self._run_job()

