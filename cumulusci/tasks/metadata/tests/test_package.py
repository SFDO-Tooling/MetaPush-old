from unittest import mock
import os
import unittest

from cumulusci.core.config import BaseGlobalConfig
from cumulusci.core.config import BaseProjectConfig
from cumulusci.core.config import TaskConfig
from cumulusci.core.config import OrgConfig
from cumulusci.tasks.metadata.package import metadata_sort_key
from cumulusci.tasks.metadata.package import BaseMetadataParser
from cumulusci.tasks.metadata.package import BundleParser
from cumulusci.tasks.metadata.package import BusinessProcessParser
from cumulusci.tasks.metadata.package import CustomLabelsParser
from cumulusci.tasks.metadata.package import CustomObjectParser
from cumulusci.tasks.metadata.package import DocumentParser
from cumulusci.tasks.metadata.package import MetadataFilenameParser
from cumulusci.tasks.metadata.package import MetadataFolderParser
from cumulusci.tasks.metadata.package import MetadataParserMissingError
from cumulusci.tasks.metadata.package import MetadataXmlElementParser
from cumulusci.tasks.metadata.package import MissingNameElementError
from cumulusci.tasks.metadata.package import PackageXmlGenerator
from cumulusci.tasks.metadata.package import ParserConfigurationError
from cumulusci.tasks.metadata.package import RecordTypeParser
from cumulusci.tasks.metadata.package import UpdatePackageXml
from cumulusci.utils import temporary_dir
from cumulusci.utils import touch

__location__ = os.path.dirname(os.path.realpath(__file__))


class TestPackageXmlGenerator(unittest.TestCase):
    def test_metadata_sort_key(self):
        md = ["a__Test__c", "Test__c"]
        md.sort(key=metadata_sort_key)
        self.assertEqual(["Test__c", "a__Test__c"], md)

    def test_package_name_urlencoding(self):
        api_version = "36.0"
        package_name = "Test & Package"

        expected = '<?xml version="1.0" encoding="UTF-8"?>\n'
        expected += '<Package xmlns="http://soap.sforce.com/2006/04/metadata">\n'
        expected += "    <fullName>Test %26 Package</fullName>\n"
        expected += "    <version>{}</version>\n".format(api_version)
        expected += "</Package>"

        with temporary_dir() as path:
            generator = PackageXmlGenerator(path, api_version, package_name)
            package_xml = generator()

        self.assertEqual(package_xml, expected)

    def test_namespaced_report_folder(self):
        api_version = "36.0"
        package_name = "Test Package"
        test_dir = "namespaced_report_folder"

        path = os.path.join(__location__, "package_metadata", test_dir)

        generator = PackageXmlGenerator(path, api_version, package_name)
        with open(os.path.join(path, "package.xml"), "r") as f:
            expected_package_xml = f.read().strip()
        package_xml = generator()

        self.assertEqual(package_xml, expected_package_xml)

    def test_delete_namespaced_report_folder(self):
        api_version = "36.0"
        package_name = "Test Package"
        test_dir = "namespaced_report_folder"

        path = os.path.join(__location__, "package_metadata", test_dir)

        generator = PackageXmlGenerator(path, api_version, package_name, delete=True)
        with open(os.path.join(path, "destructiveChanges.xml"), "r") as f:
            expected_package_xml = f.read().strip()
        package_xml = generator()

        self.assertEqual(package_xml, expected_package_xml)

    def test_parse_types_unknown_md_type(self):
        with temporary_dir() as path:
            os.mkdir(os.path.join(path, "bogus"))
            generator = PackageXmlGenerator(path, "43.0", "Test Package")
            with self.assertRaises(MetadataParserMissingError):
                generator.parse_types()

    def test_render_xml__managed(self):
        with temporary_dir() as path:
            generator = PackageXmlGenerator(
                path,
                "43.0",
                "Test Package",
                managed=True,
                install_class="Install",
                uninstall_class="Uninstall",
            )
            result = generator()
            self.assertEqual(EXPECTED_MANAGED, result)


EXPECTED_MANAGED = """<?xml version="1.0" encoding="UTF-8"?>
<Package xmlns="http://soap.sforce.com/2006/04/metadata">
    <fullName>Test Package</fullName>
    <postInstallClass>Install</postInstallClass>
    <uninstallClass>Uninstall</uninstallClass>
    <version>43.0</version>
</Package>"""


class TestBaseMetadataParser(unittest.TestCase):
    def test_parse_items__skips_files(self):
        with temporary_dir() as path:
            # create files that should be ignored by the parser
            for filename in (
                ".hidden",
                "CODEOWNERS",
                "OWNERS",
                "test.wrongext",
                "test-meta.xml",
                "Account.object",
                "Custom__c.object",
            ):
                touch(filename)

            parser = BaseMetadataParser("TestMDT", path, "object", delete=True)
            parser.parse_item = mock.Mock()
            parser.parse_items()
            parser.parse_item.assert_called_once()

    def test_check_delete_excludes__not_deleting(self):
        parser = BaseMetadataParser("TestMDT", None, "object", delete=False)
        self.assertFalse(parser.check_delete_excludes("asdf"))

    def test_parse_item(self):
        parser = BaseMetadataParser("TestMDT", None, "object", delete=False)
        with self.assertRaises(NotImplementedError):
            parser._parse_item("asdf")

    def test_render_xml__no_members(self):
        parser = BaseMetadataParser("TestMDT", None, "object", delete=False)
        self.assertIsNone(parser.render_xml())


class TestMetadataFilenameParser(unittest.TestCase):
    def test_parse_item(self):
        parser = MetadataFilenameParser("TestMDT", None, "object", delete=False)
        result = parser._parse_item("Test.object")
        self.assertEqual(["Test"], result)

    def test_parse_item_translates_namespace_tokens(self):
        with temporary_dir() as path:
            touch("___NAMESPACE___Foo__c.object")
            parser = MetadataFilenameParser("TestMDT", path, "object", delete=False)
            parser.parse_items()
        self.assertEqual(["%%%NAMESPACE%%%Foo__c"], parser.members)


class TestMetadataFolderParser(unittest.TestCase):
    def test_parse_item(self):
        with temporary_dir() as path:
            item_path = os.path.join(path, "Test")
            os.mkdir(item_path)
            with open(os.path.join(item_path, ".hidden"), "w"):
                pass
            with open(os.path.join(item_path, "Test.object"), "w"):
                pass
            parser = MetadataFolderParser("TestMDT", path, "object", delete=False)
            self.assertEqual(["Test", "Test/Test"], parser._parse_item("Test"))

    def test_parse_item__non_directory(self):
        with temporary_dir() as path:
            with open(os.path.join(path, "file"), "w"):
                pass
            parser = MetadataFolderParser("TestMDT", path, "object", delete=False)
            self.assertEqual([], parser._parse_item("file"))


class TestBundleParser(unittest.TestCase):
    def test_parse_item(self):
        with temporary_dir() as path:
            item_path = os.path.join(path, "Test")
            os.mkdir(item_path)
            with open(os.path.join(item_path, ".hidden"), "w"):
                pass
            # subitems should be ignored
            with open(os.path.join(item_path, "Test.object"), "w"):
                pass
            parser = BundleParser("TestMDT", path, "object", delete=False)
            self.assertEqual(["Test"], parser._parse_item("Test"))

    def test_parse_item__non_directory(self):
        with temporary_dir() as path:
            with open(os.path.join(path, "file"), "w"):
                pass
            parser = BundleParser("TestMDT", path, "object", delete=False)
            self.assertEqual([], parser._parse_item("file"))


class TestMetadataXmlElementParser(unittest.TestCase):
    def test_parser(self):
        with temporary_dir() as path:
            with open(os.path.join(path, "Test.test"), "w") as f:
                f.write(
                    """<?xml version='1.0' encoding='utf-8'?>
<root xmlns="http://soap.sforce.com/2006/04/metadata">
    <test>
        <fullName>Test</fullName>
    </test>
</root>"""
                )
            parser = MetadataXmlElementParser(
                "TestMDT", path, "test", delete=False, item_xpath="./sf:test"
            )
            result = parser()
            self.assertEqual(
                """    <types>
        <members>Test.Test</members>
        <name>TestMDT</name>
    </types>""",
                "\n".join(result),
            )

    def test_parser__missing_item_xpath(self):
        with self.assertRaises(ParserConfigurationError):
            parser = MetadataXmlElementParser("TestMDT", None, "test", False)
            self.assertIsNotNone(parser)

    def test_parser__missing_name(self):
        with temporary_dir() as path:
            with open(os.path.join(path, "Test.test"), "w") as f:
                f.write(
                    """<?xml version='1.0' encoding='utf-8'?>
<root xmlns="http://soap.sforce.com/2006/04/metadata">
    <test />
</root>"""
                )
            parser = MetadataXmlElementParser(
                "TestMDT", path, "test", delete=False, item_xpath="./sf:test"
            )
            with self.assertRaises(MissingNameElementError):
                parser()


class TestCustomLabelsParser(unittest.TestCase):
    def test_parser(self):
        with temporary_dir() as path:
            with open(os.path.join(path, "custom.labels"), "w") as f:
                f.write(
                    """<?xml version='1.0' encoding='utf-8'?>
<root xmlns="http://soap.sforce.com/2006/04/metadata">
    <labels>
        <fullName>TestLabel</fullName>
    </labels>
</root>"""
                )
            parser = CustomLabelsParser(
                "CustomLabels", path, "labels", False, item_xpath="./sf:labels"
            )
            self.assertEqual(["TestLabel"], parser._parse_item("custom.labels"))


class TestCustomObjectParser(unittest.TestCase):
    def test_parse_item(self):
        parser = CustomObjectParser("CustomObject", None, "object", False)
        self.assertEqual(["Test__c"], parser._parse_item("Test__c.object"))

    def test_parse_item__skips_namespaced(self):
        parser = CustomObjectParser("CustomObject", None, "object", False)
        self.assertEqual([], parser._parse_item("ns__Object__c.object"))

    def test_parse_item__skips_standard(self):
        parser = CustomObjectParser("CustomObject", None, "object", False)
        self.assertEqual([], parser._parse_item("Account.object"))


class TestRecordTypeParser(unittest.TestCase):
    def test_check_delete_excludes(self):
        parser = RecordTypeParser(
            "RecordType", None, "object", True, "./sf:recordTypes"
        )
        self.assertTrue(parser.check_delete_excludes("asdf"))


class TestBusinessProcessParser(unittest.TestCase):
    def test_check_delete_excludes(self):
        parser = BusinessProcessParser(
            "BusinessProcess", None, "object", True, "./sf:businessProcesses"
        )
        self.assertTrue(parser.check_delete_excludes("asdf"))


class TestDocumentParser(unittest.TestCase):
    def test_parse_subitem(self):
        parser = DocumentParser("Document", None, None, False)
        self.assertEqual(["folder/doc"], parser._parse_subitem("folder", "doc"))


class TestUpdatePackageXml(unittest.TestCase):
    def test_run_task(self):
        src_path = os.path.join(
            __location__, "package_metadata", "namespaced_report_folder"
        )
        with open(os.path.join(src_path, "package.xml"), "r") as f:
            expected = f.read()
        with temporary_dir() as path:
            output_path = os.path.join(path, "package.xml")
            project_config = BaseProjectConfig(
                BaseGlobalConfig(),
                {
                    "project": {
                        "package": {"name": "Test Package", "api_version": "36.0"}
                    }
                },
            )
            task_config = TaskConfig(
                {"options": {"path": src_path, "output": output_path, "managed": True}}
            )
            org_config = OrgConfig({}, "test")
            task = UpdatePackageXml(project_config, task_config, org_config)
            task()
            with open(output_path, "r") as f:
                result = f.read()
            self.assertEqual(expected, result)