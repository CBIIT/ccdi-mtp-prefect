import os
import S3ToLocal as S3ToLocal
import Commons as commons



def test_download_file_with_checksum_fail():
    print("Test Download file from FTP With Checksum validation")
    config = {}
    config["s3-profile"] = "mtp-opensearch"
    config["s3-bucket"] = "d3b-openaccess-us-east-1-prd-pbta"
    config["type"] = "file"
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["overwrite"] = False
    config["checksum-type"] = "MD5"
    config["checksum-matching"] = True
    config["files-to-download"] = [{
        "key": "open-targets/v12/mtp-tables/pre-release/putative-oncogene-fused-gene-freq.jsonl.gz",
        "checksum": "cd8de727601868472d29886006695c3cToFail"
    },
        {
            "key": "open-targets/v12/mtp-tables/pre-release/putative-oncogene-fusion-freq.jsonl.gz",
            "checksum": "96cdb4e80c18c1f848d60508d683955bToFail"
        }]

    # clean directory
    for file in config["files-to-download"]:
        commons.remove_file(config["save-path"] + file["key"])

    # download files
    S3ToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
        assert os.path.exists(config["save-path"] + file["key"]) == False


def test_download_file_without_checksum():
    print("Test Download file from FTP without Checksum validation")
    config = {}
    config["s3-profile"]="mtp-opensearch"
    config["s3-bucket"] = "d3b-openaccess-us-east-1-prd-pbta"
    config["type"] = "file"
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["overwrite"] = False
    config["checksum-matching"] = False
    config["files-to-download"] = [{
        "key": "open-targets/v12/mtp-tables/pre-release/putative-oncogene-fused-gene-freq.jsonl.gz"
    },
        {
            "key": "open-targets/v12/mtp-tables/pre-release/putative-oncogene-fusion-freq.jsonl.gz"
        }]
    #clean directory
    for file in config["files-to-download"]:
        commons.remove_file(config["save-path"]+file["key"])

    # download files
    S3ToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
            assert os.path.exists(config["save-path"]+file["key"]) == True



def test_download_file_with_checksum_pass():
    print("Test Download file from FTP With Checksum validation")
    config = {}
    config["s3-profile"] = "mtp-opensearch"
    config["s3-bucket"] = "d3b-openaccess-us-east-1-prd-pbta"
    config["type"] = "file"
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["overwrite"] = False
    config["checksum-type"] = "MD5"
    config["checksum-matching"] = True
    config["files-to-download"] = [{
        "key": "open-targets/v12/mtp-tables/pre-release/putative-oncogene-fused-gene-freq.jsonl.gz",
        "checksum": "cd8de727601868472d29886006695c3c"
    },
        {
            "key": "open-targets/v12/mtp-tables/pre-release/putative-oncogene-fusion-freq.jsonl.gz",
            "checksum": "96cdb4e80c18c1f848d60508d683955b"
        }]

    # clean directory
    for file in config["files-to-download"]:
        commons.remove_file(config["save-path"] + file["key"])

    # download files
    S3ToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
        assert os.path.exists(config["save-path"] + file["key"]) == True


def test_download_folder():
    config = {}
    config["s3-profile"] = "mtp-opensearch"
    config["s3-bucket"] = "d3b-openaccess-us-east-1-prd-pbta"
    config["type"] = "dir"
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["overwrite"] = False
    config["files-to-download"] = [{
        "key": "open-targets/v12/mtp-tables/pre-release/"
    }]
    # clean directory
    for file in config["files-to-download"]:
        commons.remove_file(config["save-path"] + file["key"])

    # download files
    S3ToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
        assert os.path.exists(config["save-path"] + file["key"]) == True



def test_download_folder_skip_existing_files():
    print("Test Download file from FTP With Checksum validation")
    config = {}
    config["s3-profile"] = "mtp-opensearch"
    config["s3-bucket"] = "d3b-openaccess-us-east-1-prd-pbta"
    config["type"] = "dir"
    config["save-path"] = "/Users/cheny39/Documents/work/tmp/tmp/"
    config["overwrite"] = False
    config["files-to-download"] = [{
        "key": "open-targets/v12/mtp-tables/pre-release/"
    }]
    # download files
    S3ToLocal.do_download_jobs(config)
    for file in config["files-to-download"]:
        assert os.path.exists(config["save-path"] + file["path"]) == True


