from glob import glob
import logging
import os
import unittest

import luigi
from luigi.contrib.lsf import LSFJobTask, LSFShellJob


DEFAULT_HOME = "/nfs/users/nfs_g/ge2/.tmp/luigi-test"
if not os.path.isdir(DEFAULT_HOME):
    os.makedirs(DEFAULT_HOME)

logger = logging.getLogger('luigi-interface')


class ChildLSFTask(LSFJobTask):

    i = luigi.Parameter()
    dont_remove_tmp_dir = True
    shared_tmp_dir = '/nfs/users/nfs_g/ge2/.tmp'
    no_tarball = False
    retry_count = 1

    def work(self):
        logger.info('Running test job...')
        with self.output().open('w') as f:
            f.write('this is a test\n')

    def output(self):
        return luigi.LocalTarget(os.path.join(DEFAULT_HOME,
                                              "test_" + str(self.i)))


class ParentLSFTask(LSFJobTask):

    dont_remove_tmp_dir = True
    no_tarball = False
    shared_tmp_dir = '/nfs/users/nfs_g/ge2/.tmp'

    def requires(self):
        return [ChildLSFTask(i=str(i), n_cpu=1) for i in range(3)]

    def work(self):
        with self.output().open('w') as output_handle:
            for input_file in self.input():
                output_handle.write(input_file.path)

    def output(self):
        return luigi.LocalTarget(os.path.join(DEFAULT_HOME, "summary"))


class ChildShellLSFTask(LSFShellJob):

    i = luigi.Parameter()
    shared_tmp_dir = '/nfs/users/nfs_g/ge2/.tmp'
    retry_count = 1

    def output(self):
        out = luigi.LocalTarget(os.path.join(DEFAULT_HOME,
                                "shell_test_" + str(self.i)))
        return out

    def job_cmd(self):
        return "echo 'this is a test' > %s" % self.output().path


class ParentShellLSFTask(LSFShellJob):

    shared_tmp_dir = '/nfs/users/nfs_g/ge2/.tmp'

    def requires(self):
        return [ChildShellLSFTask(i=str(i), n_cpu=1) for i in range(3)]

    def job_cmd(self):
        base = "cat "
        for output in self.input():
            base += output.path + " "
        base += "> %s" % self.output().path

        return base

    def output(self):
        out = luigi.LocalTarget(os.path.join(DEFAULT_HOME, "shell_summary"))
        logger.info("output: " + out.path)
        return out


class TestLSFJob(unittest.TestCase):

    def test_run_job(self):
        outfile = os.path.join(DEFAULT_HOME, "summary")
        # tasks = [ChildLSFTask(i=str(i), n_cpu=1) for i in range(3)]
        tasks = [ParentLSFTask()]
        luigi.build(tasks, local_scheduler=True, workers=3)

        self.assertTrue(os.path.exists(outfile))

    def test_run_shell_job(self):
        outfile = os.path.join(DEFAULT_HOME, "test_5")

        tasks = [ParentShellLSFTask()]
        luigi.build(tasks, local_scheduler=True, workers=3)

        self.assertTrue(os.path.exists(outfile))

    def tearDown(self):
        for fpath in glob(os.path.join(DEFAULT_HOME, 'test_*')):
            try:
                os.remove(fpath)
            except OSError:
                pass
