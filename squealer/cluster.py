
from java.io import IOException, PrintWriter, OutputStreamWriter

from org.apache.hadoop.fs import FileSystem
from org.apache.hadoop.fs import Path
from org.apache.pig.backend.hadoop.datastorage import ConfigurationUtil

# from org.apache.pig.test import Util

class Cluster(object):
    """
    Encapsulates all the file system operations.
    Mainly used for copying data to the test cluster.

    Note that this implementation is taken directly from PigUnit released
    with Pig 0.8
    """

    configuration = None

    def __init__(self, pig_context):
        self.configuration = ConfigurationUtil.toConfiguration(pig_context.getProperties())

    def exists(self, destination):
        fs = destination.getFileSystem(self.configuration)
        return fs.exists(destination)

    def update(self, local_path, dest_path):
        """
        If file size has changed, or if destination does not exist yet, copy it.
        local_path: Path to the local file to copy to the cluster.
        dest_path: Destination path on the cluster.
        """
        if not self.exist(dest_path) or not self.sameSize(local_path, dest_path):
            self.copyFromLocalFile(local_path, dest_path, True)

    def copyFromLocalFile(self, local_path, dest_path, overwrite = True):
        fs = local_path.getFileSystem(self.configuration)
        fs.copyFromLocalFile(False, overwrite, local_path, dest_path)

    def copyContentFromLocalFile(self, content, dest_path, overwrite = True):
        file_path = Path(dest_path)
        fs = file_path.getFileSystem(self.configuration)
        if overwrite and fs.exists(file_path):
            fs.delete(file_path, True)
        self.createInputFile(fs, dest_path, content)

    def copyMultipleFromLocalFile(self, data, overwrite = False):
        for i in xrange(len(data)):
            src = data[i][0]
            dest = data[0][1]
            self.copyFromLocalFile(src, dest, overwrite)

    def listStatus(self, path):
        fs = path.getFileSystem(self.configuration)
        return fs.listStatus(path)

    def delete(self, path):
        fs = path.getFileSystem(self.configuration)
        return fs.delete(path, True)

    def sameSize(self, local, dest):
        fs1 = FileSystem.getLocal(self.configuration)
        fs2 = dest.getFileSystem(self.configuration)
        return fs1.getFileStatus(local).getLen() == fs2.getFileStatus(dest).getLen()
    

    def createInputFile(self, fs, fileName, input_data):
        if(fs.exists(Path(fileName))):
            raise IOException("File " + fileName + " already exists on the minicluster")
        stream = fs.create(Path(fileName))
        pw = PrintWriter(OutputStreamWriter(stream, "UTF-8"))
        for i in xrange(len(input_data)):
            pw.println(input_data[i])
        pw.close();




