import os
import random
import shlex
import socket
import subprocess
import tempfile
import unittest
import filecmp

from ngi_pipeline.utils.filesystem import chdir, curdir_tmpdir, do_rsync, execute_command_line, \
                                          load_modules, safe_makedir, do_hardlink, do_symlink, \
                                          locate_flowcell, locate_project

class TestFilesystemUtils(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = tempfile.mkdtemp()


    def test_locate_flowcell(self):
        flowcell_name = "temp_flowcell"
        tmp_dir = tempfile.mkdtemp()
        config = {'environment': {'flowcell_inbox': [tmp_dir]}}
        with self.assertRaises(ValueError):
            # Should raise ValueError if flowcell can't be found
            locate_flowcell(flowcell=flowcell_name, config=config)

        tmp_flowcell_path = os.path.join(tmp_dir, flowcell_name)
        with self.assertRaises(ValueError):
            # Should raise ValueError as path given doesn't exist
            locate_flowcell(flowcell=tmp_flowcell_path, config=config)

        os.makedirs(tmp_flowcell_path)
        # Should return the path passed in
        self.assertEqual(locate_flowcell(flowcell=tmp_flowcell_path, config=config),
                         tmp_flowcell_path)

        # Should return the full path after searching flowcell_inbox
        self.assertEqual(locate_flowcell(flowcell=flowcell_name, config=config),
                         tmp_flowcell_path)


    def test_locate_project(self):
        project_name = "temp_project"
        tmp_dir = tempfile.mkdtemp()
        sub_dir = "DATA"
        config = {'analysis': {
            'top_dir': os.path.join('top', 'dir'),
            'base_root': tmp_dir,
            'sthlm_root': 'sthlm',
            'upps_root': 'uppsala'}}

        # raise a ValueError if a required key is missing from the config
        for leave_out in ['top_dir', 'base_root', 'sthlm_root']:
            with self.assertRaises(ValueError):
                locate_project(project=project_name,
                               config={'analysis': {k: v for k, v in config['analysis'] if k != leave_out}})

        # Should raise ValueError if project can't be found
        with self.assertRaises(ValueError):
            locate_project(project=project_name, config=config)

        project_path = os.path.join(config['analysis']['base_root'],
                                    config['analysis']['sthlm_root'],
                                    config['analysis']['top_dir'],
                                    sub_dir,
                                    project_name)
        with self.assertRaises(ValueError):
            # Should raise ValueError if a given path doesn't exist
            locate_project(project=project_path, config=config)

        os.makedirs(project_path)
        # Should return the path passed in
        self.assertEqual(locate_project(project=project_path, config=config),
                         project_path)

        # Should return the full path after searching project data dir
        self.assertEqual(locate_project(project=project_name, config=config, subdir=sub_dir),
                         project_path)

        # a project should be found also under the upps_root
        project_name = "temp_upps_project"
        project_path = os.path.join(config['analysis']['base_root'],
                                    config['analysis']['upps_root'],
                                    config['analysis']['top_dir'],
                                    sub_dir,
                                    project_name)
        os.makedirs(project_path)
        self.assertEqual(locate_project(project=project_name, config=config, subdir=sub_dir),
                         project_path)



    def test_load_modules(self):
        modules_to_load = ['R/3.1.0', 'java/sun_jdk1.7.0_25']
        load_modules(modules_to_load)
        assert(subprocess.check_output(shlex.split("R --version")).split()[2] == "3.1.0")

    def test_execute_command_line(self):
        cl = "hostname"
        popen_object = execute_command_line(cl, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        reported_hostname = popen_object.communicate()[0].strip()
        assert(reported_hostname == socket.gethostname())

    def test_execute_command_line_RuntimeError(self):
        cl = "nosuchcommand"
        with self.assertRaises(RuntimeError):
            execute_command_line(cl)

    def test_do_rsync(self):
        src_dir = tempfile.mkdtemp()
        dst_dir = tempfile.mkdtemp()
        file_names = ["file1.txt", "file2.txt"]
        src_file_paths = [os.path.join(src_dir, file_name) for file_name in file_names]
        dst_file_paths = [os.path.join(dst_dir, file_name) for file_name in file_names]
        for src_file_path in src_file_paths:
            open(src_file_path, 'w').close()
        do_rsync(src_file_paths, dst_dir)
        for dst_file_path in dst_file_paths:
            assert(os.path.exists(dst_file_path))

    def test_do_links(self):
        src_tmp_dir = tempfile.mkdtemp()
        dst_tmp_dir = os.path.join(src_tmp_dir, 'dst' )
        safe_makedir(dst_tmp_dir)
        src_file_path = os.path.join(src_tmp_dir, 'file1.txt') 
        dst_file_path = os.path.join(dst_tmp_dir, 'file1.txt') 
        open(src_file_path, 'w').close()
        do_hardlink([src_file_path], dst_tmp_dir)
        assert(filecmp.cmp(src_file_path, dst_file_path))
        os.remove(dst_file_path)
        do_symlink([src_file_path], dst_tmp_dir)
        assert(filecmp.cmp(src_file_path, dst_file_path))

    def test_safe_makedir_singledir(self):
        # Should test that this doesn't overwrite an existing dir as well
        single_dir = os.path.join(self.tmp_dir, "single_directory")
        safe_makedir(single_dir)
        assert(os.path.exists(single_dir))

    def test_safe_makedir_dirtree(self):
        dir_tree = os.path.join(self.tmp_dir, "first", "second", "third")
        safe_makedir(dir_tree)
        assert(os.path.exists(dir_tree))

    def test_curdir_tmpdir(self):
        with curdir_tmpdir() as new_tmp_dir:
            assert(os.path.exists(new_tmp_dir))
        assert(not os.path.exists(new_tmp_dir)), "Directory is not properly removed after creation"

    def test_chdir(self):
        original_dir = os.getcwd()
        new_directory = os.path.join(original_dir, self.tmp_dir)
        with chdir(self.tmp_dir):
            assert(os.getcwd() == new_directory), "New directory does not match intended one"
        assert(os.getcwd() == original_dir), "Original directory is not returned to after context manager is closed"
