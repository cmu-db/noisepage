import requests

from util.constants import LOG


class Jenkins:
    def __init__(self, url):
        self.base_url = url

    def get_artifacts(self, folders, project, branch, min_build=0, status_filter=None):
        """ Return a list of artifact dict objects from a set of builds related
        to a Jenkins job.
        Args:
            folders (arr[str]): The sub folders where the Jenkins job exists
            proect (str): The Jenkins project associated with the job
            branch (str): The Jenkins branch for the job
            min_build (int): The lowest build number to return results from. If
                             none is specified it will search all builds
            status_filter (str): Specify a filter if you only want to return 
                                 results that completed with a certain status
        Return:
            artifacts (arr[dict]): The list complete list of artifacts for all
                                   builds
        """
        artifacts = []
        job = self.get_job_data(folders, project, branch)
        build_numbers = get_build_numbers_from_job(
            job, min_build, status_filter)
        for build_num in build_numbers:
            try:
                build = self.get_build_data(
                    folders, project, branch, build_num)
                artifact_paths = get_artifact_paths_from_build(build)
                for artifact_path in artifact_paths:
                    try:
                        artifacts.append(self.get_artifact_data(
                            folders, project, branch, build_num, artifact_path))
                    except:
                        pass
            except:
                pass
        return artifacts

    def get_job_data(self, folders, project, branch):
        """ Returns a dict with details about the Jenkins job. This contains
        information about the builds associtaed with the job """
        job_url = "{BASE_URL}/{JOB_PATH}/api/json".format(
            BASE_URL=self.base_url, JOB_PATH=create_job_path(folders, project, branch))
        try:
            LOG.debug(
                "Retrieving list of Jenkins builds from {URL}".format(URL=job_url))
            response = requests.get(job_url)
            response.raise_for_status()
            return response.json()
        except Exception as err:
            LOG.error("Unexpected error when retrieving list of Jenkins builds")
            LOG.error(err)
            raise

    def get_build_data(self, folders, project, branch, build_number=1):
        """ Returns a dict with details about the Jenkins build. This contains
        information about the artifacts associtaed with the build """
        build_url = "{BASE_URL}/{JOB_PATH}/{BUILD_NUM}/api/json".format(
            BASE_URL=self.base_url, JOB_PATH=create_job_path(folders, project, branch), BUILD_NUM=build_number)
        try:
            LOG.debug(
                "Retrieving Jenkins JSON build data from {URL}".format(URL=build_url))
            response = requests.get(build_url)
            response.raise_for_status()
            return response.json()
        except Exception as err:
            LOG.error("Unexpected error when retrieving Jenkins build data")
            LOG.error(err)
            raise

    def get_artifact_data(self, folders, project, branch, build_number, artifact_path):
        """ Returns a dict with details about the Jenkins artifact. The
        contents of the artifact must be JSON for this to work. """
        artifact_url = "{BASE_URL}/{JOB_PATH}/{BUILD_NUM}/artifact/{ARTIFACT}".format(BASE_URL=self.base_url,
                                                                                      JOB_PATH=create_job_path(folders, project, branch), BUILD_NUM=build_number, ARTIFACT=artifact_path)
        try:
            LOG.debug("Retrieving JSON artifact data from {URL}".format(
                URL=artifact_url))
            response = requests.get(artifact_url)
            response.raise_for_status()
            return response.json()
        except Exception as err:
            LOG.error("Unexpected error when retrieving Jenkins artifact")
            LOG.error(err)
            raise


def create_job_path(folders, project, branch=None):
    """ Create the Jenkins path based on the folders, project, and branch
    provided """
    job_paths = []
    for folder in folders:
        job_paths.append("job/{FOLDER}/".format(FOLDER=folder))
    job_path = job_paths.append("job/{PROJ}".format(PROJ=project))
    if branch:
        job_paths.append("/job/{BRANCH}".format(BRANCH=branch))
    return ''.join(job_paths)


def get_build_numbers_from_job(job_dict, min_build=0, status_filter=None):
    """ Given a dict containing the job details return a list of build numbers.
    The build numbers will be >= min_build and will satisfy the status_filter
    if provided. """
    build_numbers = []
    for build in job_dict.get('builds', []):
        if build.get('number') >= min_build and (build.get('result') == status_filter if status_filter else True):
            build_numbers.append(build.get('number'))
    return build_numbers


def get_artifact_paths_from_build(build_dict):
    """ Given a dict containing the build details return a list of artifact
    paths associated with that build """
    artifact_paths = []
    for artifact in build_dict.get('artifacts'):

        if artifact.get('relativePath').endswith('_benchmark.json'):
            artifact_paths.append(artifact.get('relativePath'))
    return artifact_paths
