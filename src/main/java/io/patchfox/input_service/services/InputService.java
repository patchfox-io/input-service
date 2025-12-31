package io.patchfox.input_service.services;


import java.io.File;
import java.io.IOException;
import java.nio.file.Path;
import java.time.ZonedDateTime;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Set;
import java.util.UUID;
import java.util.regex.Matcher;
import java.util.stream.Collectors;
import java.util.stream.Stream;

import org.apache.commons.io.FileUtils;
import org.apache.commons.io.filefilter.FileFilterUtils;
import org.apache.commons.io.filefilter.IOFileFilter;
import org.apache.commons.io.filefilter.TrueFileFilter;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.dao.DataIntegrityViolationException;
import org.springframework.data.repository.query.Param;
import org.springframework.http.HttpStatus;
import org.springframework.stereotype.Service;
import org.springframework.web.multipart.MultipartFile;

import com.fasterxml.jackson.databind.ObjectMapper;

import com.github.packageurl.MalformedPackageURLException;
import com.github.packageurl.PackageURL;

import io.patchfox.db_entities.entities.Dataset;
import io.patchfox.db_entities.entities.Datasource;
import io.patchfox.db_entities.entities.DatasourceEvent;
import io.patchfox.input_service.components.EnvironmentComponent;
import io.patchfox.input_service.controllers.InputController;
import io.patchfox.input_service.helpers.HibernateHelper;
import io.patchfox.input_service.kafka.KafkaBeans;
import io.patchfox.input_service.repositories.DatasetRepository;
import io.patchfox.input_service.repositories.DatasourceEventRepository;
import io.patchfox.input_service.repositories.DatasourceRepository;
import io.patchfox.package_utils.data.DataFile;
import io.patchfox.package_utils.data.pkg.PackageWrapper;
import io.patchfox.package_utils.data.pkg.PackageData;
import io.patchfox.package_utils.data.sbom.SbomPackageData;
import io.patchfox.package_utils.data.sbom.syft.Report;
import io.patchfox.package_utils.data.sbom.syft.SyftSbomPackageData;
import io.patchfox.package_utils.json.ApiResponse;
import io.patchfox.package_utils.util.FileHelpers;
import io.patchfox.package_utils.util.Pair;

import lombok.extern.slf4j.Slf4j;

import net.lingala.zip4j.ZipFile;


@Service
@Slf4j
public class InputService {

    @Autowired
    private HibernateHelper hibernateHelper;

    @Autowired
    private DatasourceEventRepository datasourceEventRepository;

    @Autowired 
    private DatasourceRepository datasourceRepository;

    @Autowired
    private DatasetRepository datasetRepository;

    @Autowired
    private KafkaBeans kafka;

    @Autowired
    EnvironmentComponent env;



    /**
     * 
     * @param txid
     * @param requestReceivedAt
     * @param datasourceEvent
     * @param eventFileData
     * @return
     */
    public ApiResponse handleGitEvent(
        UUID txid, 
        ZonedDateTime requestReceivedAt,
        PackageURL datasourceEvent,
        MultipartFile eventFileData
    ) {

        //
        // extract what we need.
        // controller has inspected purl fields and validated arguments are present and valid 
        //
        var datasourceDomain = datasourceEvent.getNamespace();
        var datasourcePurl = datasourceEvent.getCoordinates();
        var datasourcePackedName = datasourceEvent.getName();
        var datasourceName = datasourceEvent.getName().split("::")[0];
        var datasourceCommitBranch = datasourceEvent.getName().split("::")[1];
        var datasourceType = datasourceEvent.getVersion();
        var datasourceCommitHash = datasourceEvent.getQualifiers().get(InputController.COMMIT_HASH_QUALIFIER_KEY);

        var datasourceCommitDatetimeString = 
            datasourceEvent.getQualifiers().get(InputController.COMMIT_DATETIME_QUALIFIER_KEY);

        var datasourceCommitDatetime = ZonedDateTime.parse(datasourceCommitDatetimeString);

        // 
        // grab dataset db record corresponding to this input. if one does not exist - make it 
        // update dataset db record as needed.
        //
        //var datasetRecord = hibernateHelper.fetchOrMakeAndFetchDatasetRecord(datasourceDomain, requestReceivedAt, txid);
        var datasetRecord = datasetRepository.createAndFetchOrFetchDataset(datasourceDomain, requestReceivedAt, txid);

        //
        // grab the datasource db record corresponding to this input. if one does not exist - make it 
        // update dataset db record as needed.
        // 
        // contoller ensures only acceptable name is "ALL". we don't want to trigger any other new dataset creation
        // in this flow for processing efficiency and security reasons
        //
        // var datasourceRecord = 
        //     hibernateHelper.fetchOrMakeAndFetchDatasourceRecord(
        //         datasetRecord,
        //         datasourcePurl, 
        //         datasourceDomain,
        //         datasourcePackedName,
        //         datasourceType,
        //         datasourceCommitBranch,
        //         requestReceivedAt, 
        //         txid
        //     ); 
        

        var datasourceRecord = 
            datasourceRepository.createAndFetchOrFetchDatasource(
                setToSqlArrayString(Set.of(datasetRecord.getId())),
                datasourceCommitBranch,
                datasourceDomain,
                requestReceivedAt,
                txid,
                datasourcePackedName,
                datasourcePurl,
                datasourceType,
                ","
            );

        // check to see if we need to add a new record to the Dataset table 
        // remember - "Datasets" are containers for "Datasources"
        //            "Datasources" are containers for "DataSourceEvents" 
        var matchList = datasetRecord.getDatasources()
                                     .stream()
                                     .filter(datasource -> datasource.getName().equals(datasourceName))
                                     .toList();

        if (matchList.isEmpty()) {
            datasetRecord.getDatasources().add(datasourceRecord);
            datasetRecord = datasetRepository.save(datasetRecord);
        }

        // now we are certain the record exists in dataset table we can update timestamp
        datasetRecord.setUpdatedAt(requestReceivedAt);
        datasetRecord = datasetRepository.save(datasetRecord);


        //
        // attempt to serialize the file data to safe tmp dir on filesystem
        //
        
        Pair<Path, Path> serializedPathPair = new Pair<>(null, null);
        try {
            serializedPathPair = FileHelpers.safeSerializeFile(
                                                 eventFileData.getOriginalFilename(), 
                                                 eventFileData.getInputStream()
                                             );
        } catch (IOException e) {
            log.error("something went wrong serializing caller supplied file: {0}", eventFileData.getOriginalFilename());
            var lastEventReceivedStatus = HttpStatus.BAD_REQUEST.getReasonPhrase();
            return hibernateHelper.recordErrorAndGetApiResponse(
                datasourceRecord,
                lastEventReceivedStatus,
                txid,
                requestReceivedAt
            );
        }


        // 
        // attempt to process the serialized data into something wrapped by our standard wrapper objects 
        //
        var datasourceEventRecord = DatasourceEvent.builder()
                                                   //.datasets(new HashSet<>(Set.of(datasetRecord))) 
                                                   .datasource(datasourceRecord)
                                                   .purl(datasourceEvent.toString())
                                                   .txid(txid)
                                                   .commitHash(datasourceCommitHash)
                                                   .commitBranch(datasourceCommitBranch)
                                                   .commitDateTime(datasourceCommitDatetime)
                                                   .eventDateTime(requestReceivedAt)
                                                   // INGESTING means we've got the event but we need to do 
                                                   // additional processing before it's ready for the 
                                                   // orchestrate-service to pick up 
                                                   .status(DatasourceEvent.Status.INGESTING)
                                                   .build();     

        try {

            Map<String, List<DataFile>> projectsMap = processZipFile(serializedPathPair);

            // there's only going to be one key in there. These methods were ported from the alpha version where
            // things worked by uploading a zip file with [n] projects in it. As such the name of each project was baked 
            // into the filepath of each path in the zip archive. Presently, the "key" will end up being the git commit 
            // hash for the data being sent to us. We want the map keyed to the repository (project) name. Given we're 
            // moving fast and trying desperately not to break too many things we're going to leave the ported methods 
            // alone and work around these little bits of odd. 
            //
            // it's important we do this so parseProjectsMap() correctly populates the resultant PackageWrapper obj with 
            // the correct project name.
            var key = projectsMap.keySet().stream().findAny().get();
            projectsMap = Map.of(datasourceName, projectsMap.get(key));
            log.debug("projectsMap: {}", projectsMap);
            PackageWrapper p = parseProjectsMap(datasourceEvent, projectsMap).get(0);
            log.info("completed parsing for project: {}", p.getPurl());


            /*
             * if processing was successful we store the event  
             */
            var mapper = new ObjectMapper().findAndRegisterModules();
            var payloadBytes = mapper.writeValueAsBytes(p);
            datasourceEventRecord.setPayload(payloadBytes);
            datasourceEventRecord.setStatus(DatasourceEvent.Status.READY_FOR_PROCESSING);

            // update package table 
            // we're safe to do this at this point because we know for sure we're dealing with the one SBOM the caller
            // sent us.  
            var sbom = p.getDependencyData().get(PackageData.PackageDataType.SBOM).get(0);
            // save packages to db if they don't already exist 
            // we're updating and saving the datasourceRecord as part of this which is why we are getting a fresh one 
            // back.
            //log.info("packages are: {}", ((SbomPackageData<SyftSbomPackageData>)sbom).getDependencyTree().getAllChildren(true));

            // check to ensure the SBOM is populated with packages. if it isn't we don't want to serialize it because 
            // it (1) is more stuff to process that won't result in value (2) it messes with the analyze-service in 
            // subtle ways.
            if (((SbomPackageData<SyftSbomPackageData>)sbom).getDependencyTree().getAllChildren(true).isEmpty()) {
                return ApiResponse.builder()
                                  .code(HttpStatus.BAD_REQUEST.value())
                                  .serverMessage(
                                    String.format(
                                 "event %s has an empty SBOM - rejecting event", 
                                        datasourceEventRecord.getPurl()
                                    )
                                  )
                                  .txid(txid)
                                  .requestReceivedAt(requestReceivedAt)
                                  .build();
            }

            // datasourceEventRecord = hibernateHelper.savePackages(
            //     datasourceEventRecord, 
            //     (SbomPackageData)sbom, 
            //     requestReceivedAt
            // );

            try {
                datasourceEventRecord = datasourceEventRepository.save(datasourceEventRecord);
                hibernateHelper.savePackages(
                    datasourceEventRecord.getId(), 
                    (SbomPackageData)sbom, 
                    requestReceivedAt
                );
            } catch (DataIntegrityViolationException e) {
                var purl = p.getPurl().toString();
                log.warn("caller attempted to add datasourceEvent {} that already occured. ", purl);     
                
                var existingDatasourceEventRecord = datasourceEventRepository.findAllByPurl(purl).get(0);
                
                // check to see if existing event recorded an error. if so allow reprocessing
                if (existingDatasourceEventRecord.getStatus() == DatasourceEvent.Status.PROCESSING_ERROR) {
                    log.info("allowing reprocessing of event because previous status was PROCESSING_ERROR");
                    datasourceEventRepository.delete(existingDatasourceEventRecord);
                    datasourceEventRecord = datasourceEventRepository.save(datasourceEventRecord); 
                // otherwise leave existing event alone and return 400 to caller
                } else {

                    return ApiResponse.builder()
                                      .code(HttpStatus.BAD_REQUEST.value())
                                      .serverMessage(
                                          String.format(
                                              "event %s already exists and has been previously processed.", 
                                              purl
                                          )
                                      )
                                      .txid(txid)
                                      .requestReceivedAt(requestReceivedAt)
                                      .build();
                }

            }
            
                
            // add event to datasource record and set ready flag 
            //
            // don't override INITIALIZING because we need to make sure we've gathered as much historical data as 
            // possible before processing - to reduce pipeline churn. 
            //
            // don't override PROCESSING because we don't want to kick off a new processing job until the whatever
            // data is currently in the queue for processing has been processed. 
            if (
                datasourceRecord.getStatus() != Datasource.Status.INITIALIZING
                && datasourceRecord.getStatus() != Datasource.Status.PROCESSING
                && datasourceRecord.getStatus() != Datasource.Status.READY_FOR_NEXT_PROCESSING
            ) {
                datasourceRecord.setStatus(Datasource.Status.READY_FOR_PROCESSING);
            }
            datasourceRecord = datasourceRepository.save(datasourceRecord);

            // same for dataset
            if (
                datasetRecord.getStatus() != Dataset.Status.INITIALIZING
                && datasetRecord.getStatus() != Dataset.Status.PROCESSING
            ) {
                datasetRecord.setStatus(Dataset.Status.READY_FOR_PROCESSING);
            }
            datasetRecord = datasetRepository.save(datasetRecord);

            //
            // and we're out 
            //

        } catch (Exception e) {
            log.error("caught exception while processing: ", e);

            // this is probably due to the caller attempting to upload the same event more than once 
            if (e instanceof IllegalArgumentException || e instanceof DataIntegrityViolationException) {
                var lastEventReceivedStatus = HttpStatus.BAD_REQUEST.getReasonPhrase();
                // no need to update dataset "updatedAt" field as we did so already 
                return hibernateHelper.recordErrorAndGetApiResponse(
                    datasourceRecord,
                    lastEventReceivedStatus,
                    txid,
                    requestReceivedAt
                );
            } else {
                var lastEventReceivedStatus = HttpStatus.INTERNAL_SERVER_ERROR.getReasonPhrase();
                // no need to update dataset "updatedAt" field as we did so already 
                return hibernateHelper.recordErrorAndGetApiResponse(
                    datasourceRecord,
                    lastEventReceivedStatus,
                    txid,
                    requestReceivedAt
                );
            }

        } finally {
            // clean up after ourselves 
            FileHelpers.safeDeletePath(serializedPathPair.getLeft());
        }

        // bye
        return ApiResponse.builder()
                          .code(HttpStatus.ACCEPTED.value())
                          .txid(txid)
                          .requestReceivedAt(requestReceivedAt)
                          .build();
    }


    /**
     * takes location of serialized data (uploaded by caller), unpacks the file, and processes valid bundles of 
     * project (code repository) metadata into a map pairing project to a list of pairs where each pair<L, R> is
     * <string indicating type of file, List<DataFile objects wrapping the recognized code project data files>
     * 
     * --WORK LOG --
     * 
     * date   -$> 19NOV23
     * ticket -=> https://trello.com/c/9UdtccAZ
     * PR     -+> https://github.com/patchfox-io/wintermute/pull/20
     * 
     *      this got totally overhauled again to accept the "DataFile" wrapper instead of the typed pair of 
     *      <DepGraphNode, Report> where "Report" was specific to a single scanner's result. "DataFile" can 
     *      instead encapsulate [n] types of result.
     * 
     * 
     * date   -$> 07OCT23
     * ticket -=> https://trello.com/c/J9UN8RBD
     * PR     -+> https://github.com/patchfox-io/wintermute/pull/4
     * 
     *      depending on the host OS (ex wsl2/ubuntu vs actual ubuntu) the files may be read in differing order. 
     *      the previous catch for dumping the read-file-buffer was the size of the buffer but in some cases 
     *      there may be an unexpected number of files (ex - both a gradle and maven build metadata file). in 
     *      those cases the old logic would fail if the files in the buffer happened to be read in an order that 
     *      did not include all the necessary files in the set because there happened to the the right 
     *      number of them. 
     * 
     *      this new logic ensures there are AT LEAST the expected number of files and the set of 
     *      necessary files. It will also pick up on any other files it recognizes and ignore the rest. 
     * 
     * 
     * @param serializedPathPair
     * @return
     */
    Map<String, List<DataFile>> processZipFile(Pair<Path, Path> serializedPathPair) {
        Path workingDirPath = serializedPathPair.getLeft();
        Path zipFilePath = serializedPathPair.getRight();

        List<DataFile.DataFileTypeEnum> necessaryFiles = Stream.of(
            DataFile.DataFileTypeEnum.BUILD_FILE_GIT_BLAME, 
            //DataFile.DataFileTypeEnum.OWASP_DEPENDENCY_CHECK,
            DataFile.DataFileTypeEnum.SYFT_SBOM,
            DataFile.DataFileTypeEnum.ETL_BUILD_METADATA
            //DataFile.DataFileTypeEnum.GRYPE_OSS
        ).collect(Collectors.toList());

        // try-with-resources to ensure ZipFile is closed()
        // https://docs.oracle.com/javase/tutorial/essential/exceptions/tryResourceClose.html
        try (ZipFile zipFile = new ZipFile(zipFilePath.toString());) {
            zipFile.extractAll(workingDirPath.toString());
        } catch (IOException e) {
            log.error("something went wrong unpacking the zip file", e);
            throw new IllegalArgumentException();
        } 

        //
        String zipFileName = zipFilePath.getFileName().toString();
        IOFileFilter zipFileFilterMatch = FileFilterUtils.nameFileFilter(zipFileName);
        IOFileFilter zipFileFilter = FileFilterUtils.notFileFilter(zipFileFilterMatch);

        // @TODO figure out why this isn't filtering out the parent directory
        // IOFileFilter parentDirFilterMatch = FileFilterUtils.nameFileFilter(workingDirPath.toString());
        // IOFileFilter parentDirFilter = FileFilterUtils.notFileFilter(parentDirFilterMatch);
        Collection<File> fileList = FileUtils.listFilesAndDirs(
                                        workingDirPath.toFile(),
                                        zipFileFilter,
                                        TrueFileFilter.INSTANCE
                                    );

        // we want a map of lists because we want the ability to associate multiple build-system artifacts with
        // the same project
        Map<String, List<DataFile>> projectsMap = new HashMap<>();
        List<DataFile> discoveredFilesTmpBuffer = new ArrayList<>();
        List<DataFile.DataFileTypeEnum> discoveredFileKeysTmpBuffer = new ArrayList<>();

        String lastProjectName = "";
        for (File file : fileList) {
            if (file.isDirectory()) { continue; }
            String projectName = "";
            log.debug("unpacked: {}", file);
            log.debug("file.getName() is: {}", file.getName());
            log.debug("file absolute path is: {}", file.getAbsolutePath());

            // split the path by whatever system tmp is
            // ex 
            //      /tmp/eaa7b1dc-b965-4a46-83f5-a0a6608f202c/gradle_project_sample/gradle_depgraph.txt
            //      [, "/eaa7b1dc-b965-4a46-83f5-a0a6608f202c/gradle_project_sample/gradle_depgraph.txt"]
            String[] tempDirSplit = file.getAbsolutePath().split(FileHelpers.TEMP_DIR_PATH);
            
            // if there aren't two elements it means the the path didn't start with system tmp
            if (tempDirSplit.length != 2) { 
                log.warn("file {} should be in system tmp directory and isn't!", file.getAbsolutePath());
                continue; 
            }
            
            // grab what's hanging off system tmp and split again by system path seperator
            // ex - the above result becomes 
            //      [, eaa7b1dc-b965-4a46-83f5-a0a6608f202c, gradle_project_sample, dependency-check-report.json]
            String path = tempDirSplit[1];
            String systemFileSeparator = FileHelpers.SYSTEM_FILE_SEPARATOR; 
            var matcher = Matcher.quoteReplacement(systemFileSeparator);
            String[] splitFilePath = path.split(matcher);
            log.debug("path is: {}", path);
            log.debug("splitFilePath is: {}", java.util.Arrays.toString(splitFilePath));

            // here the length should be greater than three because 
            //      the first element is an empty string due to the file separator being the thing we split on
            //      the second element is the subdir of tmp we made that should be a uuid
            //      the third element is the project name
            //      the fourth element is probably the file we're looking for 
            //
            // it's ok if the fourth element isn't the file - what's important is that there are more than three 
            // elements in splitfilePath as if there's fewer there's no projectname to parse
            if (splitFilePath.length > 3) { projectName = splitFilePath[2]; } 

            log.debug("projectName is: {}", projectName);
            log.debug("lastProjectName is: {}", lastProjectName);
            log.debug("discoveredFilesTmpBuffer: {}", discoveredFilesTmpBuffer);             

            //
            // check to see if we need to flush the buffers
            //

            // if first run through - set lastProjectName to current
            if (lastProjectName.equals("")) { 
                lastProjectName = projectName; 
            }
            // else if lastProjectName doesn't equal projectName we check to see if the buffer is ready to be processed.
            // for that to happen: 
            //   * all the necessary files for a properly formed set of input are present 
            //   * the buffer has the minimum number of expected files (ie - what's necessary + some kind of build file)
            else if (lastProjectName.equals(projectName) == false) {
            
                boolean buffersProcessedFlag = checkAndClearDiscoveredFileBuffers(
                                                    discoveredFileKeysTmpBuffer, 
                                                    necessaryFiles, 
                                                    projectsMap, 
                                                    discoveredFilesTmpBuffer, 
                                                    lastProjectName
                                                );

                // otherwise - if the lastProjectName doesn't equal project name and we didn't handle it above then 
                // some kind of input got in the buffer that we can't process.\
                // clear the buffers and move on with life... 
                if (buffersProcessedFlag == false) {
                    discoveredFilesTmpBuffer.clear();
                    discoveredFileKeysTmpBuffer.clear();
                }

                // make sure we do this regardless or the logic will get dorked up next go-round 
                lastProjectName = projectName;

            }               


            //
            // process the file into the buffer if it's one we're looking for
            //

            if (DataFile.isRecognizedFileType(file)) {
                var dataFile = new DataFile(file, projectName);
                discoveredFilesTmpBuffer.add(dataFile);
                discoveredFileKeysTmpBuffer.add(dataFile.getFileType());
            }

        }

        //
        // cleanup anything left in the buffer
        //

        // if this pops it's because there was some file data that didn't meet processing standards at the tail of the
        // projects list 
        if (discoveredFileKeysTmpBuffer.size() > 0) {
            log.debug("file buffer not empty. attempting to process. project name: {}", lastProjectName);
            boolean buffersProcessedFlag = checkAndClearDiscoveredFileBuffers(
                                    discoveredFileKeysTmpBuffer, 
                                    necessaryFiles, 
                                    projectsMap, 
                                    discoveredFilesTmpBuffer, 
                                    lastProjectName
                                );

            // otherwise - if the lastProjectName doesn't equal project name and we didn't handle it above then 
            // some kind of input got in the buffer that we can't process. put a warn message in the log and 
            // move on ...
            if (buffersProcessedFlag == false) {
                log.warn("refusing to process data for project: {} because the bundle is malformed", lastProjectName);
                discoveredFilesTmpBuffer.clear();
                discoveredFileKeysTmpBuffer.clear();
            }
        }


        //
        // see you space cowboy...
        //

        return projectsMap;
    }


    /**
     * helper method to determine whether or not to update projectsMap wtih the contents of the tmp buffers. returns
     * boolean indicating whether or not the buffer contents were transferred to the projectsMap. if this happened, the
     * buffers are cleared as a result. 
     * 
     * @param discoveredFileKeysTmpBuffer
     * @param necessaryFiles
     * @param projectsMap
     * @param discoveredFilesTmpBuffer
     * @param projectKey
     * @return
     */
    boolean checkAndClearDiscoveredFileBuffers(
            List<DataFile.DataFileTypeEnum> discoveredFileKeysTmpBuffer,
            List<DataFile.DataFileTypeEnum> necessaryFiles,
            Map<String, List<DataFile>> projectsMap,
            List<DataFile> discoveredFilesTmpBuffer,
            String projectKey
        ) {

        // currently we expect 
        // * git blame annotated build file 
        // * syft sbom file 
        var expectedNumberFilesInEventPayload = 3;

        boolean rv = false;

        if (discoveredFileKeysTmpBuffer.containsAll(necessaryFiles)
                        && discoveredFileKeysTmpBuffer.size() >= expectedNumberFilesInEventPayload) {

                    log.debug("discoveredFileKeysTmpBuffer is: {}", discoveredFileKeysTmpBuffer);
                    // make sure each data file is properly tagged with the correct project name key
                    discoveredFilesTmpBuffer.stream().forEach(df -> df.setProjectName(projectKey));
                    projectsMap.put(projectKey, new ArrayList<>(discoveredFilesTmpBuffer));
                    log.debug("putting kv {} {}", projectKey, projectsMap.get(projectKey));

                    discoveredFilesTmpBuffer.clear();
                    discoveredFileKeysTmpBuffer.clear();
                    rv = true;
        } else {
            log.warn("refusing to process data for project: {} because the bundle is malformed", projectKey);
        }

        return rv;
    }


    /**
     * processes a projectName keyed map of DataFile lists and converts them to PatchFox representations of both 
     * the dependency graph but also all the metadata associated with the code project. 
     * 
     *  --WORK LOG --
     * 
     *  date   -$> 19NOV23
     *  ticket -=> https://trello.com/c/9UdtccAZ
     *  PR     -+> https://github.com/patchfox-io/wintermute/pull/20
     * 
     *      gained significant code reduction by migrating the processing logic to the DataFile object itself. This 
     *      method need only know what kind of file it's looking at (which DataFile reports) in order to capture and
     *      properly cast the resultant data object. 
     * 
     *      it's possible in this flow for there to be more than one DepGraph file in the data set. in that case 
     *      only the last one processed will be recorded. In a project there won't ever be more than one "active"
     *      dependency manager. The rare cases where this happens is when David is experimenting with projects that
     *      convert maven to gradle files and visa versa - which results in two build files in the same project
     *      directory.
     * 
     * @param projectsMap
     * @return
     */
    List<PackageWrapper> parseProjectsMap(PackageURL eventDataSource, Map<String, List<DataFile>> projectsMap) {    
        List<PackageWrapper> rv = new ArrayList<>();

        for (String key : projectsMap.keySet()) {
            String projectName = eventDataSource.getName();
            PackageWrapper dummy = PackageWrapper.getDummyWithFields(
                eventDataSource.getType(),
                eventDataSource.getNamespace(),
                eventDataSource.getName(),
                eventDataSource.getVersion()
            ).get();
            dummy.setProjectName(projectName);
            PackageWrapper rootNode = dummy;
            List<PackageData> dependencyDataList = new ArrayList<>();

            log.info("processing project data for: {}", projectName);
            List<DataFile> projectData = projectsMap.get(key);
            for (DataFile dataFile : projectData) {    

                try {
                    String className = dataFile.returns();
                    log.info("processing data file type: {}", dataFile.getFileType());
                    log.debug("dataFile.returns() is: {}", className);
                    log.debug("classname packagewrapper? {}", className.equals(PackageWrapper.CLASSNAME));
                    log.debug("classname packagedata? {}", className.equals(PackageData.CLASSNAME));
                    switch(className) {
                        case (PackageWrapper.CLASSNAME):
                            rootNode = (PackageWrapper)dataFile.process().get();
                            break;
                        case (PackageData.CLASSNAME):
                            dependencyDataList.add((PackageData)dataFile.process().get());
                            break;
                        default:
                            log.warn("dataFile: {} returns unrecognized type: {}", dataFile.getFileName(), className);
                            break;
                    }
                } catch (IllegalArgumentException | IOException | NoSuchElementException e) {
                    log.warn("caught unexpected exception {} while processing data file {}", e, dataFile.getFileName());                    
                }

            }

            // we might need to know this in a min
            var sbomType = PackageData.PackageDataType.SBOM;
            var sbomDataOptional = dependencyDataList.stream()
                                                     .filter(data -> data.getPackageDataType().equals(sbomType))
                                                     .findFirst();

            // if we fall in here it's because we found a dependency graph file and parsed that into the root node obj
            if ( !rootNode.equals(dummy) ) {
                rootNode.addAllDependencyData(dependencyDataList);
                rootNode.disseminateOssDataToChildren();
                rv.add(rootNode);
            }

            // if we fall into this it is because we did not find a dependency graph file and thus could not create a
            // root node obj. there's an SBOM present in the DependencyData list so we'll try to process that into the
            // root node we need. 
            //
            // also note that if we fall in here we're probably capturing all the constituents of the repository 
            // but the SBOM may not have the dependency graph information. In that case, everything will be a direct
            // child of the root node. 
            if (rootNode.equals(dummy) && sbomDataOptional.isPresent()) {
                var sbomPackageData = sbomDataOptional.get();

                try {
                    rootNode = ((SyftSbomPackageData)sbomPackageData).getDependencyTree(eventDataSource);
                    rootNode.addAllDependencyData(dependencyDataList);
                    rootNode.disseminateOssDataToChildren();
                    rootNode.setIsRoot(true);
                    rv.add(rootNode);
                } catch (MalformedPackageURLException e) {
                    log.error("could not create rootNode from sbom!");
                }
            } 

            // if we're here and rootNode is still the dummy one we made at the top we should note the event. 
            // it's not a stop the world problem - but something went wrong in processing that should be logged. 
            // there are multipe 
            if (rootNode.equals(dummy)) {
                log.debug("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
                log.debug("root node has null artifact!! projectName is: {}", projectName);
                log.debug("likely something went wrong prosessing the dependency graph file");
                log.debug("=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-=-");
            }

        }

        return rv;
    }    


    public String setToSqlArrayString(Set<Long> s) {
        return s.toString()
                .replace("[", "")
                .replace("]", "")
                .replace(" ", "");
    }

}
