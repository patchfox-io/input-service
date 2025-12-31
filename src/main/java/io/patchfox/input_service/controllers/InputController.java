package io.patchfox.input_service.controllers;


import java.time.ZonedDateTime;
import java.time.format.DateTimeParseException;
import java.util.HashSet;
import java.util.Set;
import java.util.UUID;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpStatus;
import org.springframework.http.MediaType;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestAttribute;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;
import org.springframework.web.multipart.MultipartFile;

import com.github.packageurl.PackageURL;

import io.patchfox.input_service.components.EnvironmentComponent;
import io.patchfox.input_service.services.InputService;
import io.patchfox.package_utils.json.ApiResponse;
import lombok.extern.slf4j.Slf4j;


    /* -= On Datasources and DatasourceEvents =-
     *
     * "DatasourceEvent" is a superset of what we mean when we say "Datasource". A "Datasource" is only the name of
     * the source of data. when we talk about "DatasourceEvent" we're include more information like the git 
     * commit branch, the commit hash, the datetime of the commit, etc. 
     *
     * PatchFox uses the purl spec to refer to software packages - ie "dependencies" We also use the same to refer 
     * to the sources of said packages. 
     * 
     * 
     * 
     * The purl format looks like this:
     *      scheme:type/namespace/name@version?qualifiers#subpath
     * 
     * 
     * 
     * a PatchFox Datasource purl looks like: 
     *      pkg:generic/{DOMAIN}/{DATASOURCE}@{DATASOURCE_TYPE}
     * 
     * for a git Datasource the purl looks like: 
     *      pkg:generic/{DOMAIN}/{DATASOURCE}::{COMMIT_BRANCH}@{DATASOURCE_TYPE}    
     * 
     * "scheme" and "type" will always be "pkg" and "generic" respectively
     * 
     * {DOMAIN} is the name assigned to the client deployment - ie - https://{DOMAIN}.patchfox.io
     * 
     * {DATASOURCE} is exactly what it sounds like - the name of the thing pumping data into the pipeline. Usually 
     * this is the name of a git repository
     * 
     * {DATASOURCE_TYPE} is probably a purl type. It may in future include other types. Its function is to serve as
     * an indication to PF what kind of data comes out of a given Datasource and thus by extension how to process
     * that data. See https://github.com/package-url/purl-spec/blob/master/PURL-TYPES.rst for list of purl types.
     * 
     * {COMMIT_BRANCH} is the git branch from which this data came. Only present for git Datasources
     * 
     * example:
     *     pkg:generic/acme/foo-service::main@npm
     * 
     * 
     * 
     * a PatchFox DatasourceEvent purl looks like:
     *      pkg:generic/{DOMAIN}/{DATASOURCE}@{DATASOURCE_TYPE}?commitHash={COMMIT_HASH}&commitDatetime={COMMIT_DATETIME}
     * 
     * {COMMIT_HASH} is the git commit hash from which this data came
     *  
     * {COMMIT_DATETIME} is an ISO formatted datetime string indicating when the commit occured from which this 
     * data came
     * 
     * example:
     *     pkg:generic/acme/foo-service::main@npm?commitHash=d5dd8011d1a55038262533675eddb96d98c4b984&commitDatetime=2022-05-13T22:00:56+00:00
     * 
     * 
     * 
     */


@RestController
@Slf4j
public class InputController {
    
    @Autowired
    InputService inputService;

    @Autowired
    EnvironmentComponent env;

    public static final String API_PATH_PREFIX = "/api/v1";
    public static final String INPUT_PATH = API_PATH_PREFIX + "/input";
    public static final String INPUT_GIT_PATH = INPUT_PATH + "/git"; // there will be others eg - /input/{something}
    public static final String POST_INPUT_GIT_SIGNATURE = "POST_" + INPUT_GIT_PATH;
    
    // these will always be lower cased like this 
    public static final String COMMIT_HASH_QUALIFIER_KEY = "commithash";
    public static final String COMMIT_DATETIME_QUALIFIER_KEY = "commitdatetime";

    @PostMapping(
        value = INPUT_GIT_PATH,
        produces = MediaType.APPLICATION_JSON_VALUE
    )
    ResponseEntity<ApiResponse> inputGetHandler(
        @RequestAttribute UUID txid,
        @RequestAttribute ZonedDateTime requestReceivedAt,
        @RequestParam PackageURL datasourceEvent,
        @RequestParam MultipartFile eventFileData 

    ) {
        // while we know we have a valid PackageURL (purl) object, we don't know if the fields contain
        // all the right things. current business logic dictates all of these be present in the purl.
        // we will in future branch on the results of this in case of integration with other datasource types -- ie not
        // git repos. we also want to make sure there isn't extra garbage in there. some fields are validated by 
        // PackageURL constructor. We're doing extra inspection because this endpoint is exposed to the public internet
        // and I want to make it harder to smuggle malicious shit into the app. YES this endpoint is auth protected
        // and YES we're using spring-data and JPA with no raw SQL or JQL  but a few extra layers of deterrence never 
        // hurt anyone. 
        //
        // https://github.com/package-url/purl-spec

        /*
         * eventDatasource type, namespace, name, version, and subpath fields for size and basic content sanity
         */

        // alpha/numeric/period/hyphen/underscore/forwardslash/backslash/percent/colon (for percent encoding)
        // not a great catchall but good enough for now...
        var regexFilter = "^[-a-zA-Z0-9._/\\%:]+$";

        if (
            // if this isn't "generic" someone has sent us a purl for a package, not a git repo
            datasourceEvent.getType().equals("generic") == false
                // for all non-PF deployements this is the subdomain of the deployment. 
                || datasourceEvent.getNamespace().length() > 256 == true
                || datasourceEvent.getNamespace().matches(regexFilter) == false
                || ( !env.getExpectedDomain().equals("*") && !env.getExpectedDomain().equals(datasourceEvent.getNamespace()) )
                // Datasource name is expected to be a concatination of the repository name and branch name 
                // joined by "::". Should not be too big, malformed, empty, or contain weird characters.
                || datasourceEvent.getName().contains("::") == false
                || datasourceEvent.getName().length() > 512
                || datasourceEvent.getName().split("::").length != 2
                || datasourceEvent.getName().split("::")[0].isBlank()
                || datasourceEvent.getName().split("::")[0].length() > 256
                || datasourceEvent.getName().split("::")[0].matches(regexFilter) == false
                || datasourceEvent.getName().split("::")[1].isBlank()
                || datasourceEvent.getName().split("::")[1].length() > 256
                || datasourceEvent.getName().split("::")[1].matches(regexFilter) == false
                // permissive because externally set. no non printing chars
                || datasourceEvent.getVersion().length() > 256 == true
                || datasourceEvent.getVersion().matches(regexFilter) == false
                // current business rules disallow subpath to be populated 
                || datasourceEvent.getSubpath() != null
        ) {
            // return error response - needs to be object of this type vs exception in case of kafka caller
            var payload = ApiResponse.builder()
                                     .txid(txid)
                                     .requestReceivedAt(requestReceivedAt)
                                     .code(HttpStatus.BAD_REQUEST.value())
                                     .serverMessage("DatasourceEvent purl malformed")
                                     .build();

            return wrapApiResponsePayload(payload);                 
        }


        /*
         * check eventDatasource qualifiers field 
         */

        // Set.of() produces an immutable collection that causes problems
        // also "commithash" and "commitdatetime" has to be lower cased here regardless of what caller sends us
        // I think because of how purl stores and returns qualifier names ...
        var expectedKeyset = new HashSet<>(Set.of("commithash", "commitdatetime"));
        var qualifierKeys = datasourceEvent.getQualifiers().keySet();
        log.debug("qualifierKeys is: {}", qualifierKeys);
        expectedKeyset.removeAll(qualifierKeys);
        if ( !expectedKeyset.isEmpty() ) {
            // return error response - needs to be object of this type vs exception in case of kafka caller
            var payload = ApiResponse.builder()
                                     .txid(txid)
                                     .requestReceivedAt(requestReceivedAt)
                                     .code(HttpStatus.BAD_REQUEST.value())
                                     .serverMessage("eventDatasource purl qualifiers are invalid.")
                                     .build();

            return wrapApiResponsePayload(payload);
        }

        var datasourceCommitHash = datasourceEvent.getQualifiers().get(COMMIT_HASH_QUALIFIER_KEY);
        var datasourceCommitDatetime = datasourceEvent.getQualifiers().get(COMMIT_DATETIME_QUALIFIER_KEY);


        //
        // validate datasourceCommitHash
        //

        // git uses either SHA-1 or SHA-256
        // https://git-scm.com/docs/hash-function-transition
        var isSha1 = datasourceCommitHash.matches("^[a-fA-F0-9]{40}$");
        var isSha256 = datasourceCommitHash.matches("^[a-fA-F0-9]{64}$");

        if ( !isSha1 && !isSha256) {
            // return error response - needs to be object of this type vs exception in case of kafka caller
            var payload = ApiResponse.builder()
                                     .txid(txid)
                                     .requestReceivedAt(requestReceivedAt)
                                     .code(HttpStatus.BAD_REQUEST.value())
                                     .serverMessage("qualifier datasourceCommitHash is not valid git hash.")
                                     .build();

            return wrapApiResponsePayload(payload);
        }


        //
        // validate commitDatetime
        //

        try {
            ZonedDateTime.parse(datasourceCommitDatetime);
        } catch (DateTimeParseException e) {
            // return error response - needs to be object of this type vs exception in case of kafka caller
            var payload = ApiResponse.builder()
                                     .txid(txid)
                                     .requestReceivedAt(requestReceivedAt)
                                     .code(HttpStatus.BAD_REQUEST.value())
                                     .serverMessage("qualifier datasourceCommitDatetime is not valid.")
                                     .build();

            return wrapApiResponsePayload(payload);
        }


       /*
        * expectation is data file size max is enforced by spring by way of config values in application.properties 
        * file. if we're here, all values considered valid. OK to proceed to service layer for processing. 
        */
        var apiResponse = inputService.handleGitEvent(txid, requestReceivedAt, datasourceEvent, eventFileData);
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);

    }
    

    /**
     * 
     * @param apiResponse
     */
    private ResponseEntity<ApiResponse> wrapApiResponsePayload(ApiResponse apiResponse) {
        return ResponseEntity.status(apiResponse.getCode()).body(apiResponse);
    }
}
