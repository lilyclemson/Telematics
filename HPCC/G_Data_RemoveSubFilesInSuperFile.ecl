IMPORT Std;

SuperFileName := '~levin::kafkamessagetablesuper';

//Remove sub files from Super file
RemoveSubFilesInSuperFile := ORDERED(
        STD.File.StartSuperFileTransaction(),
        STD.File.RemoveOwnedSubFiles(SuperFileName),
        STD.File.FinishSuperFileTransaction()
    );
//Get all the names of files in the super file
SubFiles := STD.File.SuperFileContents(SuperFileName);
SubFiles;
//Delete files
DeleteLogicalFileInSuperFile := APPLY(SubFiles,STD.File.DeleteLogicalFile('~'+SubFiles.name));
//Delete steps: 1.Get all the names of files in the super file; 2.Remove files from super file; 3. Delete files at hard drive
RemoveSubFilesInSuperFileSteps := ORDERED(
    RemoveSubFilesInSuperFile,
    DeleteLogicalFileInSuperFile
);
RemoveSubFilesInSuperFileSteps;




