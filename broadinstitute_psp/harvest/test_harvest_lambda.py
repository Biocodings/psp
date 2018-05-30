import unittest
import mock
from botocore.exceptions import ClientError
import logging
import broadinstitute_psp.harvest.harvest_lambda as h
import broadinstitute_psp.utils.setup_logger as setup_logger

logger = logging.getLogger(setup_logger.LOGGER_NAME)

r = mock.MagicMock()
r.ok = mock.Mock(return_value=True)
r.text = mock.Mock(return_value="passed")
h.requests.put = mock.Mock(return_value=r)

# mock setup environment variables
environDict = {"API_KEY": "API_KEY", "API_URL": "API_URL"}

def get_environ_item(name):
    return environDict[name]

h.os.environ = mock.MagicMock()
h.os.environ.__getitem__.side_effect = get_environ_item


class TestHarvestLambda(unittest.TestCase):

    @staticmethod
    def setup_panorama_request():
        req_id = "5afdf5b35a5fbe51cf571650"
        name = "LINCS_P100_DIA_Plate61_annotated_minimized_2018-02-06_17-09-06"
        panorama_request = {
            "name": name,
            "assay": "P100",
            "status": "Waiting_To_Download",
            "id": req_id,
            "level 2": {
                "panorama": {
                    "method": "GET",
                    "url": "https://panoramaweb.org/lincs/LINCS-DCIC/P100/runGCTReport.view?runId=32201&reportName=GCT%20File%20P100"
                }
            }
        }
        return (req_id, name, panorama_request)


    def test_handler(self):
        pass

    def test_harvest(self):
        #test setup
        (req_id, name, panorama_req) = TestHarvestLambda.setup_panorama_request()
        h.boto3.client = mock.Mock()
        h.urllib.urlopen = mock.Mock()
        h.post_update_to_proteomics_clue = mock.Mock()

        #unhappy url to panorama should call to urllib.urlopen, fail, and post failure to clue
        h.urllib.urlopen.side_effect = Exception("failure")

        with self.assertRaises(Exception) as context:
            h.harvest(panorama_req, "fake_bucket", "psp/level2/fake_panorama_key.json")
        self.assertEqual(str(context.exception), "failure")

        self.assertEqual(panorama_req["level 2"]["panorama"]["url"], h.urllib.urlopen.call_args[0][0])

        clue_post_args = h.post_update_to_proteomics_clue.call_args[0]
        call_url = "API_URL/"+req_id + "/level2"
        self.assertEqual("/level2", clue_post_args[0], "unhappy path, urllib Exception, post to clue does not contain API URL suffix")
        self.assertEqual(req_id, clue_post_args[1], "unhappy path, urllib Exception, post to clue does not contain request id")

        error_message = "error: " + str(context.exception)
        expected_payload = {'s3':{'message':error_message}}
        self.assertEqual(expected_payload, clue_post_args[2], "unhappy path, urllib Exception, post to clue does not contain message" )

        #unhappy s3 upload should post "s3 upload error"
        h.urllib.urlopen.side_effect = None
        h.urllib.urlopen.return_value = True
        h.s3 = mock.Mock()
        h.s3.upload_fileobj = mock.Mock()
        h.s3.upload_fileobj.side_effect= Exception(ClientError)
        h.post_update_to_proteomics_clue.reset_mock()


        with self.assertRaises(ClientError) as context:
            h.harvest(panorama_req, "fake_bucket", "psp/level2/fake_panorama_key.json")
        # self.assertEqual()
        print str(context.exception)

        clue_post_args = h.post_update_to_proteomics_clue.call_args[0]

        self.assertEqual("/level2", clue_post_args[0], "unhappy path, s3 upload Exception, post to clue does not contain API url suffix")
        self.assertEqual(req_id, clue_post_args[1], "unhappy path, s3 upload Exception, post to clue does not contain request id")


    def test_extract_data_from_panorama_request(self):
        (req_id, name, panorama_req) = TestHarvestLambda.setup_panorama_request()

        key = "psp/level2/request.json"

        (request_id, aws_key) = h.extract_data_from_panorama_request(panorama_request=panorama_req, key=key)

        self.assertEqual(req_id, request_id)
        self.assertEqual(aws_key, "psp/level2/"+name+"_LVL2"+h.FILE_EXTENSION)


    def test_post_update_to_proteomics_clue(self):
        (req_id , _ , _)= TestHarvestLambda.setup_panorama_request()

        payload = {"status": "test successfully updated API"}
        r = h.post_update_to_proteomics_clue("", req_id, payload)
        args, kwargs =  h.requests.put.call_args
        # print args, kwargs

        call_url = "API_URL/"+req_id
        self.assertEqual(call_url, args[0], "post to clue does not contain API URL to panorama req")

        self.assertEqual({'user_key':'API_KEY'}, kwargs['headers'], "post to clue does not contain headers")
        self.assertEqual(payload, kwargs['json'], "post to clue does not contain payload")


if __name__ == "__main__":
    setup_logger.setup(verbose=True)

    unittest.main()