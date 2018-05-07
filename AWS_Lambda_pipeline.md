#PSP in the cloud
##Fundamentals
PSP in cloud consists of a combination of Lambda and Batch functions, with the Batch functions utilizing the cmap/psp Docker.
cmap/psp is set up with two bash functions: 
* dry (called dry.sh in dry directory)
* tear (called tear.sh in tear directory)
both expect the following arguments: 

--bucket_name
--file_key
--config_dir { points to broadinstitute_psp.psp_production.cfg }
--plate_api_id
--plate_name

These scripts parse arguments provided by Batch, activate the psp environment set up in the Docker, run setup_psp.py and call their 
respective handlers (dry_handler and tear_handler). 

##Breakdown of Processing Steps
###Harvest
harvest.harvest_lambda is triggered on POST to proteomics.clue.io/api/psp, it parses the POST request to download the Level2 GCT 
from Panorama into the proteomics.clue.io bucket under the level 2 subdirectory, which is also where the POST JSON resides
The upload of "*.gct" in the level 2 subdirectory triggers dry
###Dry
dry.launch_dry_batch uses the name of the trigger file to get the original Panorama POST request, which it uses to instantiate 
a Batch job with the correct arguments. dry_handler is called by dry.sh and is responsible for calling dry.py and monitoring the output. 
If dry.py runs successfully, dry_handler updates the API status, and puts the resulting LVL3 GCT in the level 3 subdirectory on s3
The upload of a "*.gct" in the level 3 subdirectory triggers tear
###Tear
tear.launch_tear_batch uses the name of the trigger file to get the original Panorama POST request, which it uses to instantiate 
a Batch job with the correct arguments. tear_handler is called by tear.sh and is responsible for calling tear.py and monitoring the output. 
If tear.py runs successfully, tear_handler updates the API status, and puts the resulting LVL4 GCT in the level 4 subdirectory on s3
The upload of a "*.gct" in the level 4 subdirectory triggers pour
###Pour
pour.py uses the name of the trigger file to get the original Panorama POST request, which it uses to call to the API. 
The API has been updated along the way to include current s3 locations for each level, as well as PUT locations originally provided.
It parses the JSON to read in the appropriate s3 file and make separate PUT requests to the Panorama output location dictated
in the Panorama POST request. 