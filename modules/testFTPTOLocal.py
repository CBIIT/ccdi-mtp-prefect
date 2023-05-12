import os
import FTPToLocal as FTPToLocal
import Commons as commons

def test_download_file_without_checksum():
    print("Test Download file from FTP without Checksum validation")
    config={}
    config["host-name"] = "ftp.ebi.ac.uk"
    config["is_anonymous"] = True
    config["username"] = "admin"
    config["password"] = "admin"
    config["checksum-matching"] = False
    config["checksum-type"] = "SHA-1"
    config["overwrite"] = True
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["type"] = "file"
    config["file_extension"] = "n/a"
    config["files-to-download"] = [{
        "path":"pub/databases/opentargets/platform/23.02/input/gene-ontology-inputs/go.obo"
    }
    ]

    #clean directory
    for file in config["files-to-download"]:
        commons.remove_file(config["save-path"]+file["path"])

    # download files
    FTPToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
            assert os.path.exists(config["save-path"]+file["path"]) == True



def test_download_file_with_checksum_pass():
    print("Test Download file from FTP With Checksum validation")
    config = {}
    config["host-name"] = "ftp.ebi.ac.uk"
    config["is_anonymous"] = True
    config["username"] = "admin"
    config["password"] = "admin"
    config["checksum-matching"] = True
    config["checksum-type"] = "SHA-1"
    config["overwrite"] = True
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["type"] = "file"
    config["file_extension"] = "json"
    config["files-to-download"] = [{
        "path": "pub/databases/opentargets/platform/23.02/output/etl/json/targets/part-00156-a58b02a9-0daa-4cf0-aac3-20428ba520c2-c000.json",
        "checksum": "385099f668c52ea9cfcec76d81a9f6da8aa10d0e"
    }, {
        "path": "pub/databases/opentargets/platform/23.02/output/etl/json/targets/part-00049-a58b02a9-0daa-4cf0-aac3-20428ba520c2-c000.json",
        "checksum": "bb81e0a25227b3fa8219969addda90eacee951fa"
    }
    ]
    # clean directory
    for file in config["files-to-download"]:
        commons.remove_file(config["save-path"] + file["path"])

    # download files
    FTPToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
        assert os.path.exists(config["save-path"] + file["path"]) == True


def test_download_file_with_checksum_fail():
    print("Test Download file from FTP With Checksum validation")
    config = {}
    config["host-name"] = "ftp.ebi.ac.uk"
    config["is_anonymous"] = True
    config["username"] = "admin"
    config["password"] = "admin"
    config["checksum-matching"] = True
    config["checksum-type"] = "SHA-1"
    config["overwrite"] = True
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["type"] = "file"
    config["file_extension"] = "json"
    config["files-to-download"] = [{
        "path": "pub/databases/opentargets/platform/23.02/output/etl/json/targets/part-00156-a58b02a9-0daa-4cf0-aac3-20428ba520c2-c000.json",
        "checksum": "385099f668c52ea9cfcec76d81a9f6da8aa10d0eTestFail"
    }, {
        "path": "pub/databases/opentargets/platform/23.02/output/etl/json/targets/part-00049-a58b02a9-0daa-4cf0-aac3-20428ba520c2-c000.json",
        "checksum": "bb81e0a25227b3fa8219969addda90eacee951faTestFail"
    }
    ]

    # clean directory
    for file in config["files-to-download"]:
        commons.remove_file(config["save-path"] + file["path"])

    # download files
    FTPToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
        assert os.path.exists(config["save-path"] + file["path"]) == False


def test_download_folder():
    print("Test Download file from FTP With Checksum validation")
    config = {}
    config["host-name"] = "ftp.ebi.ac.uk"
    config["is_anonymous"] = True
    config["username"] = "admin"
    config["password"] = "admin"
    config["checksum-matching"] = False
    config["checksum-type"] = "SHA-1"
    config["overwrite"] = True
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["type"] = "dir"
    config["file_extension"] = "n/a"
    config["files-to-download"] = [{
        "path": "pub/databases/opentargets/platform/23.02/input/gene-ontology-inputs/",
    }
    ]

    # clean directory
    for file in config["files-to-download"]:
        commons.remove_file(config["save-path"] + file["path"])

    # download files
    FTPToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
        assert os.path.exists(config["save-path"] + file["path"]) == True



def test_download_folder_skip_existing_files():
    print("Test Download file from FTP With Checksum validation")
    config = {}
    config["host-name"] = "ftp.ebi.ac.uk"
    config["is_anonymous"] = True
    config["username"] = "admin"
    config["password"] = "admin"
    config["checksum-matching"] = False
    config["checksum-type"] = "SHA-1"
    config["overwrite"] = False
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["type"] = "dir"
    config["file_extension"] = "n/a"
    config["files-to-download"] = [{
        "path": "pub/databases/opentargets/platform/23.02/input/gene-ontology-inputs/",
    }
    ]

    # download files
    FTPToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
        assert os.path.exists(config["save-path"] + file["path"]) == True


def test_login_ftp_anonymous():
        ftp_server=FTPToLocal.connect("ftp.ebi.ac.uk")
        assert ftp_server.getwelcome() != ""

def test_login_ftp_username_pwd():
    # secret_usr_block = Secret.load("ccdi-ftp-chop-username-yizhen")
    # # Access the stored secret
    # userName = secret_usr_block.get()
    # secret_pwd_block = Secret.load("ccdi-ftp-chop-pwd-yizhen")
    # # Access the stored secret
    # password = secret_pwd_block.get()
    # ftp_server=FTPToLocal.connect("transfer2.chop.edu", False, userName, password)
    # assert ftp_server.getwelcome() != ""
    print("TBD")