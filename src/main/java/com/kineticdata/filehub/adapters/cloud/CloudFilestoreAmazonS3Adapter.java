package com.kineticdata.filehub.adapters.cloud;

import com.kineticdata.commons.v1.config.ConfigurableProperty;
import com.kineticdata.commons.v1.config.ConfigurablePropertyMap;
import java.util.Map;
import org.jclouds.ContextBuilder;
import org.jclouds.blobstore.BlobStoreContext;
import org.jclouds.http.HttpRequest;

public class CloudFilestoreAmazonS3Adapter extends CloudFilestoreAdapter {
    
    /** Defines the adapter display name. */
    public static final String NAME = "Amazon s3";
    
    /** Defines the collection of property names for the adapter. */
    public static class Properties {
        public static final String ACCESS_KEY = "Access Key";
        public static final String BUCKET = "Bucket";
        public static final String SECRET_ACCESS_KEY = "Secret Access Key";
    }
    
    /** 
     * Specifies the configurable properties for the adapter.  These are populated as part of object
     * construction so that the collection of properties (default values, menu options, etc) are 
     * available before the adapter is configured.  These initial properties can be used to 
     * dynamically generate the list of configurable properties, such as when the Kinetic Filehub
     * application prepares the new Filestore display.
     */
    private final ConfigurablePropertyMap properties = new ConfigurablePropertyMap(
        new ConfigurableProperty(Properties.ACCESS_KEY)
            .setIsRequired(true),
        new ConfigurableProperty(Properties.SECRET_ACCESS_KEY)
            .setIsRequired(true)
            .setIsSensitive(true),
        new ConfigurableProperty(Properties.BUCKET)
            .setIsRequired(true)
    );
    
    
    /*----------------------------------------------------------------------------------------------
     * CONFIGURATION
     *--------------------------------------------------------------------------------------------*/
    
    /**
     * Initializes the filestore adapter.  This method will be called when the properties are first
     * specified, and when the properties are updated.
     * 
     * @param propertyValues 
     */
    @Override
    public void initialize(Map<String, String> propertyValues) {
        // Set the configurable properties
        properties.setValues(propertyValues);
    }

    /**
     * Returns the display name for the adapter.
     * 
     * @return 
     */
    @Override
    public String getName() {
        return NAME;
    }

    /**
     * Returns the collection of configurable properties for the adapter.
     * 
     * @return 
     */
    @Override
    public ConfigurablePropertyMap getProperties() {
        return properties;
    }
    
    
    /*----------------------------------------------------------------------------------------------
     * IMPLEMENTATION METHODS
     *--------------------------------------------------------------------------------------------*/

    @Override
    protected BlobStoreContext buildBlobStoreContext() {
        return ContextBuilder.newBuilder("aws-s3")
            .credentials(
                properties.getValue(Properties.ACCESS_KEY), 
                properties.getValue(Properties.SECRET_ACCESS_KEY))
            .buildView(BlobStoreContext.class);
    }

    @Override
    protected String getContainer() {
        return properties.getValue(Properties.BUCKET);
    }

    @Override
    protected boolean supportsUploadMultipart() {
        return true;
    }

    @Override
    protected boolean supportsUploadStream() {
        return false;
    }
    
    
    // TODO: Remove this override and use the super implementation once 
    // jclouds supports adding parameters to signed requests.
    //   https://issues.apache.org/jira/browse/JCLOUDS-1016
    @Override
    public boolean supportsRedirectDelegation() {
        return false;
    }
    
    @Override
    public String getRedirectDelegationUrl(String path, String friendlyFilename) {
        // Declare the result
        String result;
        
        // Build the redirect delegation url
        try (
            BlobStoreContext context = buildBlobStoreContext()
        ) {
            // Build the pre signed request valid for 5 seconds
            HttpRequest request = context.getSigner().signGetBlob(
                getContainer(), path, 5);

            // Add the S3 path parameter to set the Content-Disposition header so the downloaded attachment
            // has the original file name instead of the random file name it is stored as.
            // http://docs.aws.amazon.com/AmazonS3/latest/API/RESTObjectGET.html#RESTObjectGET-requests
            //
            // NOTE: Apparently not yet possible (2016-01-07) to add this using jclouds:
            //   https://issues.apache.org/jira/browse/JCLOUDS-1016
//            result = request.getEndpoint().toString()
//                + "&response-content-disposition=inline;filename="
//                + UrlEscapers.urlFragmentEscaper().escape(friendlyFilename);
            
            // Set the result
            result = request.getEndpoint().toString();
        }
        // Return the result
        return result;
    }
}
