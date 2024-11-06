import os
import random
import shlex
import socket
import subprocess
import tempfile
import unittest
import filecmp
import mock

from ngi_pipeline.utils.filesystem import (
    chdir,
    execute_command_line,
    load_modules,
    safe_makedir,
    do_hardlink,
    do_symlink,
    locate_flowcell,
    locate_project,
    is_index_file,
)


class TestFilesystemUtils(unittest.TestCase):
    def setUp(self):
        self.tmp_dir = os.path.realpath(tempfile.mkdtemp())

    def test_locate_flowcell(self):
        flowcell_name = "temp_flowcell"
        tmp_dir = tempfile.mkdtemp()
        config = {"environment": {"flowcell_inbox": [tmp_dir]}}
        with self.assertRaises(ValueError):
            # Should raise ValueError if flowcell can't be found
            locate_flowcell(flowcell=flowcell_name, config=config)

        tmp_flowcell_path = os.path.join(tmp_dir, flowcell_name)
        with self.assertRaises(ValueError):
            # Should raise ValueError as path given doesn't exist
            locate_flowcell(flowcell=tmp_flowcell_path, config=config)

        os.makedirs(tmp_flowcell_path)
        # Should return the path passed in
        self.assertEqual(
            locate_flowcell(flowcell=tmp_flowcell_path, config=config),
            tmp_flowcell_path,
        )

        # Should return the full path after searching flowcell_inbox
        fc = locate_flowcell(flowcell=flowcell_name, config=config)
        self.assertEqual(fc, tmp_flowcell_path)

    def test_locate_project(self):
        project_name = "temp_project"
        tmp_dir = tempfile.mkdtemp()
        sthlm_root = "sthlm_root"
        top_dir = "top_dir"
        config = {
            "analysis": {
                "base_root": tmp_dir,
                "sthlm_root": sthlm_root,
                "top_dir": top_dir,
            }
        }
        with self.assertRaises(ValueError):
            # Should raise ValueError if project can't be found
            locate_project(project=project_name, config=config)

        tmp_project_path = os.path.join(
            tmp_dir, sthlm_root, top_dir, "DATA", project_name
        )
        with self.assertRaises(ValueError):
            # Should raise ValueError as path given doesn't exist
            locate_project(project=tmp_project_path, config=config)

        os.makedirs(tmp_project_path)
        # Should return the path passed in
        self.assertEqual(
            locate_project(project=tmp_project_path, config=config), tmp_project_path
        )

        # Should return the full path after searching project data dir
        self.assertEqual(
            locate_project(project=project_name, config=config), tmp_project_path
        )

    @mock.patch("ngi_pipeline.utils.filesystem.shlex.split")
    def test_load_modules(self, mock_split):
        mock_split.return_value = ["echo", 'os.environ["TEST"] = "test";']
        modules_to_load = ["Any/module"]
        load_modules(modules_to_load)
        set_envar = os.environ.get("TEST")
        self.assertEqual(set_envar, "test")

    def test_execute_command_line(self):
        cl = "hostname"
        popen_object = execute_command_line(
            cl, stdout=subprocess.PIPE, stderr=subprocess.PIPE
        )
        reported_hostname = popen_object.communicate()[0].strip()
        assert reported_hostname == socket.gethostname()

    def test_execute_command_line_RuntimeError(self):
        cl = "nosuchcommand"
        with self.assertRaises(RuntimeError):
            execute_command_line(cl)

    def test_do_links(self):
        src_tmp_dir = tempfile.mkdtemp()
        dst_tmp_dir = os.path.join(src_tmp_dir, "dst")
        safe_makedir(dst_tmp_dir)
        src_file_path = os.path.join(src_tmp_dir, "file1.txt")
        dst_file_path = os.path.join(dst_tmp_dir, "file1.txt")
        open(src_file_path, "w").close()

        do_hardlink([src_file_path], dst_tmp_dir)
        assert filecmp.cmp(src_file_path, dst_file_path)
        os.remove(dst_file_path)

        do_symlink([src_file_path], dst_tmp_dir)
        assert filecmp.cmp(src_file_path, dst_file_path)

    def test_safe_makedir_singledir(self):
        # Should test that this doesn't overwrite an existing dir as well
        single_dir = os.path.join(self.tmp_dir, "single_directory")
        safe_makedir(single_dir)
        assert os.path.exists(single_dir)

    def test_safe_makedir_dirtree(self):
        dir_tree = os.path.join(self.tmp_dir, "first", "second", "third")
        safe_makedir(dir_tree)
        assert os.path.exists(dir_tree)

    def test_chdir(self):
        original_dir = os.path.realpath(os.getcwd())
        with chdir(self.tmp_dir):
            self.assertEqual(
                self.tmp_dir,
                os.path.realpath(os.getcwd()),
                "New directory does not match intended one",
            )
        self.assertEqual(
            original_dir,
            os.path.realpath(os.getcwd()),
            "Original directory is not returned to after context manager is closed",
        )

    def test_is_index_file(self):
        self.assertTrue(is_index_file("jkhdsfajhkdsjk_L002_I2_"))
        self.assertTrue(is_index_file("jkhdsfajhkdsjk_L009_I9_jkhdhjgsdh"))
        self.assertFalse(is_index_file("jkhdsfajhkdsjk_L002_R2_"))
        self.assertFalse(is_index_file("jkhdsfajhkdsjk_L0021_I2_"))
        self.assertFalse(is_index_file("jkhdsfajhkdsjk_L002_I22_"))
        self.assertTrue(
            is_index_file("jkhdsfajhkdsjk_L002_I22_", index_file_pattern=r"_I\d\d_")
        )
        self.assertFalse(is_index_file("jkhdsfajhkdsjk"))
