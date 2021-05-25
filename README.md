# mindstreams-bci-data-service

This service interfaces with the Emotiv device and subscribes to raw EEG data. It then converts the EEG data (after filtering and aggregations) into the MindStreams Cogntivie ClickStream format. It publishes the results to Amazon Kinesis for processing.
