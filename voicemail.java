package voicemail;

import com.amazonaws.regions.Regions;

import com.amazonaws.services.lambda.runtime.Context
import com.amazonaws.services.lambda.runtime.RequestHandler
import com.amazonaws.services.lambda.runtime.LambdaLogger
import com.amazonaws.services.lambda.runtime.events.KinesisEvent;

import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.client.builder.AwsClientBuilder;

import com.amazonaws.kinesisvideo.parser.ebml.MkvTypeInfos;
import com.amazonaws.kinesisvideo.parser.mkv.Frame;
import com.amazonaws.kinesisvideo.parser.mkv.MkvDataElement;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElement;
import com.amazonaws.kinesisvideo.parser.mkv.MkvElementVisitException;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTrackMetadata;
import com.amazonaws.kinesisvideo.parser.mkv.MkvStartMasterElement;
import com.amazonaws.kinesisvideo.parser.mkv.MkvValue;
import com.amazonaws.kinesisvideo.parser.mkv.StreamingMkvReader;
import com.amazonaws.kinesisvideo.parser.utilities.FragmentMetadataVisitor;
import com.amazonaws.kinesisvideo.parser.utilities.MkvTag;

import com.amazonaws.services.kinesisvideo.AmazonKinesisVideo;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoClientBuilder;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoMedia;
import com.amazonaws.services.kinesisvideo.AmazonKinesisVideoMediaClientBuilder;

import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.PutObjectResult;

import com.amazonaws.services.kinesisvideo.model.APIName;
import com.amazonaws.services.kinesisvideo.model.GetDataEndpointRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaRequest;
import com.amazonaws.services.kinesisvideo.model.GetMediaResult;
import com.amazonaws.services.kinesisvideo.model.StartSelector;
import com.amazonaws.services.kinesisvideo.model.StartSelectorType;
import org.apache.commons.lang3.Validate;

import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.nio.ByteBuffer;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.nio.file.Files;
import java.util.Optional;

import javax.sound.sampled.AudioFileFormat;
import javax.sound.sampled.AudioFormat;
import javax.sound.sampled.AudioInputStream;
import javax.sound.sampled.AudioSystem;
import javax.sound.sampled.UnsupportedAudioFileException;

import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import voicemail.model.KVStreamRecordingData;
import voicemail.model.ContactTraceRecord;

import com.amazonaws.SdkClientException;

import static com.amazonaws.util.StringUtils.isNullOrEmpty;

public class KVSProcessRecordingLambda implements RequestHandler<KinesisEvent, String> {

    private static final Regions REGION = Regions.fromName(System.getenv("APP_REGION"));
    private static final Regions TRANSCRIBE_REGION = Regions.fromName(System.getenv("TRANSCRIBE_REGION"));
    private static final String START_SELECTOR_TYPE = System.getenv("START_SELECTOR_TYPE");
    private static final String RECORDINGS_BUCKET_NAME = System.getenv("RECORDINGS_BUCKET_NAME");
    private static final String RECORDINGS_KEY_PREFIX = System.getenv("RECORDINGS_KEY_PREFIX");
    private static final boolean RECORDINGS_PUBLIC_READ_ACL = Boolean
            .parseBoolean(System.getenv("RECORDINGS_PUBLIC_READ_ACL"));

    private static final Logger logger = LoggerFactory.getLogger(KVSProcessRecordingLambda.class);

    @Override
    public String handleRequest(KinesisEvent kinesisEvent, Context context) {
        System.out.println("Processing CTR Event");

        for (KinesisEvent.KinesisEventRecord record : kinesisEvent.getRecords()) {
            try {
                String recordData = new String(record.getKinesis().getData().array());
                System.out.println("Record Data: " + recordData);
                this.processData(recordData);
            } catch (Exception e) {
                // if json does not contain required data, will exit early
                System.out.println(e.toString());
            }
        }

        return "{ \"result\": \"Success\" }";
    }

    private boolean processData(String data) {
        JSONObject json = new JSONObject(data);
        ContactTraceRecord traceRecord = new ContactTraceRecord(json);
        List<KVStreamRecordingData> recordings = traceRecord.getRecordings();

        if (recordings.size() == 0) {
            return false;
        }

        KVStreamRecordingData recording = recordings.get(0);

        try {
            this.processAudioStream(
                    recording.getLocation(),
                    recording.getFragmentStartNumber(),
                    traceRecord.getContactId());
            return true;
        } catch (Exception e) {
            logger.error("KVS to Transcribe Streaming failed with: ", e);
            return false;
        }
    }

    public void processAudioStream(
            String streamARN,
            String startFragmentNum,
            String contactId) throws Exception {

        logger.info(String.format("StreamARN=%s, startFragmentNum=%s, contactId=%s", streamARN, startFragmentNum,
                contactId));

        long unixTime = System.currentTimeMillis() / 1000L;
        Path saveAudioFilePath = Paths.get("/tmp", contactId + "_" + unixTime + ".raw");
        System.out.println(
                String.format("Save Path: %s Start Selector Type: %s", saveAudioFilePath, START_SELECTOR_TYPE));
        FileOutputStream fileOutputStream = new FileOutputStream(saveAudioFilePath.toString());
        String streamName = streamARN.substring(streamARN.indexOf("/") + 1, streamARN.lastIndexOf("/"));

        InputStream kvsInputStream = this.getInputStreamFromKVS(streamName, REGION, startFragmentNum,
                getAWSCredentials(), START_SELECTOR_TYPE);
        StreamingMkvReader streamingMkvReader = StreamingMkvReader
                .createDefault(new InputStreamParserByteSource(kvsInputStream));

        FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor = new FragmentMetadataVisitor.BasicMkvTagProcessor();
        FragmentMetadataVisitor fragmentVisitor = FragmentMetadataVisitor.create(Optional.of(tagProcessor));

        try {
            logger.info("Saving audio bytes to location");

            // Write audio bytes from the KVS stream to the temporary file
            ByteBuffer audioBuffer = this.getByteBufferFromStream(streamingMkvReader, fragmentVisitor, tagProcessor,
                    contactId);
            while (audioBuffer.remaining() > 0) {
                byte[] audioBytes = new byte[audioBuffer.remaining()];
                audioBuffer.get(audioBytes);
                fileOutputStream.write(audioBytes);
                audioBuffer = this.getByteBufferFromStream(streamingMkvReader, fragmentVisitor, tagProcessor,
                        contactId);
            }

        } finally {
            logger.info(String.format("Closing file and upload raw audio for contactId: %s ", contactId));
            this.closeFileAndUploadRawAudio(
                    kvsInputStream, fileOutputStream, saveAudioFilePath, contactId, unixTime);
        }
    }

    /**
     * Fetches the next ByteBuffer of size 1024 bytes from the KVS stream by parsing
     * the frame from the MkvElement
     * Each frame has a ByteBuffer having size 1024
     *
     * @param streamingMkvReader
     * @param fragmentVisitor
     * @param tagProcessor
     * @param contactId
     * @return
     * @throws MkvElementVisitException
     */
    public static ByteBuffer getByteBufferFromStream(StreamingMkvReader streamingMkvReader,
            FragmentMetadataVisitor fragmentVisitor,
            FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor,
            String contactId) throws MkvElementVisitException {

        while (streamingMkvReader.mightHaveNext()) {
            Optional<MkvElement> mkvElementOptional = streamingMkvReader.nextIfAvailable();
            if (mkvElementOptional.isPresent()) {

                MkvElement mkvElement = mkvElementOptional.get();
                mkvElement.accept(fragmentVisitor);

                // Validate that we are reading data only for the expected contactId at start of
                // every mkv master element
                if (MkvTypeInfos.EBML.equals(mkvElement.getElementMetaData().getTypeInfo())) {
                    if (mkvElement instanceof MkvStartMasterElement) {
                        String contactIdFromStream = getContactIdFromStreamTag(tagProcessor);
                        if (contactIdFromStream != null && !contactIdFromStream.equals(contactId)) {
                            // expected Connect ContactId does not match the actual ContactId. End the
                            // streaming by
                            // returning an empty ByteBuffer
                            return ByteBuffer.allocate(0);
                        }
                        tagProcessor.clear();
                    }
                } else if (MkvTypeInfos.SIMPLEBLOCK.equals(mkvElement.getElementMetaData().getTypeInfo())) {
                    MkvDataElement dataElement = (MkvDataElement) mkvElement;
                    Frame frame = ((MkvValue<Frame>) dataElement.getValueCopy()).getVal();
                    ByteBuffer audioBuffer = frame.getFrameData();
                    long trackNumber = frame.getTrackNumber();
                    MkvTrackMetadata metadata = fragmentVisitor.getMkvTrackMetadata(trackNumber);

                    return audioBuffer;
                }
            }
        }
        return ByteBuffer.allocate(0);
    }

    /**
     * Closes the FileOutputStream and uploads the Raw audio file to S3
     *
     * @param kvsInputStream
     * @param fileOutputStream
     * @param saveAudioFilePath
     * @throws IOException
     */
    private void closeFileAndUploadRawAudio(InputStream kvsInputStream, FileOutputStream fileOutputStream,
            Path saveAudioFilePath, String contactId,
            long unixTime) throws IOException {

        kvsInputStream.close();
        fileOutputStream.close();

        logger.info(String.format("File size: %d", new File(saveAudioFilePath.toString()).length()));

        // Upload the Raw Audio file to S3
        if (new File(saveAudioFilePath.toString()).length() > 0) {
            S3UploadInfo uploadInfo = uploadRawAudio(REGION, RECORDINGS_BUCKET_NAME, RECORDINGS_KEY_PREFIX,
                    saveAudioFilePath.toString(), contactId, RECORDINGS_PUBLIC_READ_ACL, this.getAWSCredentials());

        } else {
            logger.info("Skipping upload to S3. audio file has 0 bytes: " + saveAudioFilePath);
        }
    }

    /**
     * @return AWS credentials to be used to connect to s3 (for fetching and
     *         uploading audio) and KVS
     */
    private static AWSCredentialsProvider getAWSCredentials() {
        return DefaultAWSCredentialsProviderChain.getInstance();
    }

    /**
     * Makes a GetMedia call to KVS and retrieves the InputStream corresponding to
     * the given streamName and startFragmentNum
     *
     * @param streamName             Stream Name
     * @param region                 Stream Region
     * @param startFragmentNum       Starting Fragment Number when recording started
     * @param awsCredentialsProvider Credential
     * @param startSelectorType      Where the stream should start at. See
     *                               StartSelectorType.
     * @return InputStream
     */

    public static InputStream getInputStreamFromKVS(String streamName,
            Regions region,
            String startFragmentNum,
            AWSCredentialsProvider awsCredentialsProvider,
            String startSelectorType) {
        Validate.notNull(streamName);
        Validate.notNull(region);
        Validate.notNull(startFragmentNum);
        Validate.notNull(awsCredentialsProvider);

        AmazonKinesisVideo amazonKinesisVideo = AmazonKinesisVideoClientBuilder.standard().build();

        String endPoint = amazonKinesisVideo.getDataEndpoint(new GetDataEndpointRequest()
                .withAPIName(APIName.GET_MEDIA)
                .withStreamName(streamName)).getDataEndpoint();

        AmazonKinesisVideoMediaClientBuilder amazonKinesisVideoMediaClientBuilder = AmazonKinesisVideoMediaClientBuilder
                .standard()
                .withEndpointConfiguration(new AwsClientBuilder.EndpointConfiguration(endPoint, region.getName()))
                .withCredentials(awsCredentialsProvider);
        AmazonKinesisVideoMedia amazonKinesisVideoMedia = amazonKinesisVideoMediaClientBuilder.build();

        StartSelector startSelector;
        startSelectorType = isNullOrEmpty(startSelectorType) ? "NOW" : startSelectorType;
        switch (startSelectorType) {
            case "FRAGMENT_NUMBER":
                startSelector = new StartSelector()
                        .withStartSelectorType(StartSelectorType.FRAGMENT_NUMBER)
                        .withAfterFragmentNumber(startFragmentNum);
                logger.info("StartSelector set to FRAGMENT_NUMBER");
                break;
            case "NOW":
            default:
                startSelector = new StartSelector()
                        .withStartSelectorType(StartSelectorType.NOW);
                logger.info("StartSelector set to NOW");
                break;
        }

        GetMediaResult getMediaResult = amazonKinesisVideoMedia.getMedia(new GetMediaRequest()
                .withStreamName(streamName)
                .withStartSelector(startSelector));

        logger.info("GetMedia called on stream {} response {} requestId {}", streamName,
                getMediaResult.getSdkHttpMetadata().getHttpStatusCode(),
                getMediaResult.getSdkResponseMetadata().getRequestId());

        return getMediaResult.getPayload();
    }

    /**
     * Iterates thorugh all the tags and retrieves the Tag value for "ContactId" tag
     *
     * @param tagProcessor
     * @return
     */
    private static String getContactIdFromStreamTag(FragmentMetadataVisitor.BasicMkvTagProcessor tagProcessor) {
        Iterator iter = tagProcessor.getTags().iterator();
        while (iter.hasNext()) {
            MkvTag tag = (MkvTag) iter.next();
            if ("ContactId".equals(tag.getTagName())) {
                return tag.getTagValue();
            }
        }
        return null;
    }

    /**
     * Converts the given raw audio data into a wav file. Returns the wav file back.
     */
    private static File convertToWav(String audioFilePath) throws IOException, UnsupportedAudioFileException {
        File outputFile = new File(audioFilePath.replace(".raw", ".wav"));
        AudioInputStream source = new AudioInputStream(Files.newInputStream(Paths.get(audioFilePath)),
                new AudioFormat(8000, 16, 1, true, false), -1); // 8KHz, 16 bit, 1 channel, signed, little-endian
        AudioSystem.write(source, AudioFileFormat.Type.WAVE, outputFile);
        return outputFile;
    }

    /**
     * Saves the raw audio file as an S3 object
     *
     * @param region
     * @param bucketName
     * @param keyPrefix
     * @param audioFilePath
     * @param awsCredentials
     */
    public static S3UploadInfo uploadRawAudio(Regions region, String bucketName, String keyPrefix, String audioFilePath,
            String contactId, boolean publicReadAcl,
            AWSCredentialsProvider awsCredentials) {
        File wavFile = null;
        S3UploadInfo uploadInfo = null;

        try {

            AmazonS3Client s3Client = (AmazonS3Client) AmazonS3ClientBuilder.standard()
                    .withRegion(region)
                    .withCredentials(awsCredentials)
                    .build();

            wavFile = convertToWav(audioFilePath);

            // upload the raw audio file to the designated S3 location
            String objectKey = keyPrefix + wavFile.getName();

            logger.info(String.format("Uploading Audio: to %s/%s from %s", bucketName, objectKey, wavFile));
            PutObjectRequest request = new PutObjectRequest(bucketName, objectKey, wavFile);
            ObjectMetadata metadata = new ObjectMetadata();
            metadata.setContentType("audio/wav");
            metadata.addUserMetadata("contact-id", contactId);
            request.setMetadata(metadata);

            if (publicReadAcl) {
                request.setCannedAcl(CannedAccessControlList.PublicRead);
            }

            PutObjectResult s3result = s3Client.putObject(request);

            logger.info("putObject completed successfully " + s3result.getETag());
            uploadInfo = new S3UploadInfo(bucketName, objectKey, region);

        } catch (SdkClientException e) {
            logger.error("Audio upload to S3 failed: ", e);
            throw e;
        } catch (UnsupportedAudioFileException | IOException e) {
            logger.error("Failed to convert to wav: ", e);
        } finally {
            if (wavFile != null) {
                wavFile.delete();
            }
        }

        return uploadInfo;
    }
}