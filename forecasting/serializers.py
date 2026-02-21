import json
from rest_framework import serializers

# second HW
class DatasetUploadSerializer(serializers.Serializer):
    file = serializers.FileField()
    # multipart can't reliably send nested dicts, so send JSON string
    columnMapping = serializers.CharField()  # e.g. {"timestamp":"Date","target":"Close"}

    def validate_columnMapping(self, v):
        try:
            obj = json.loads(v)
        except Exception:
            raise serializers.ValidationError("columnMapping must be valid JSON string")
        if "timestamp" not in obj or "target" not in obj:
            raise serializers.ValidationError('columnMapping must contain "timestamp" and "target"')
        return obj
# second HW
class DatasetUploadResponseSerializer(serializers.Serializer):
    datasetVersionId = serializers.CharField()
    status = serializers.CharField()
    rawUri = serializers.CharField()

class DatasetCreateSerializer(serializers.Serializer):
    name = serializers.CharField()

class DatasetCreateResponseSerializer(serializers.Serializer):
    datasetId = serializers.CharField()

class DatasetCommitSerializer(serializers.Serializer):
    localPath = serializers.CharField()
    columnMapping = serializers.DictField()  # {"timestamp":"Date","target":"Close"}

class DatasetCommitResponseSerializer(serializers.Serializer):
    datasetVersionId = serializers.CharField()
    status = serializers.CharField()

class DatasetVersionSerializer(serializers.Serializer):
    datasetVersionId = serializers.CharField()
    status = serializers.CharField()
    checksum = serializers.CharField(allow_null=True)
    schema = serializers.DictField()
    profile = serializers.DictField()
    rawUri = serializers.CharField()
    processedUri = serializers.CharField(allow_null=True)
    errorMessage = serializers.CharField(allow_null=True)

class ForecastCreateSerializer(serializers.Serializer):
    datasetVersionId = serializers.CharField()
    modelType = serializers.CharField()
    params = serializers.DictField(required=False)
    horizon = serializers.IntegerField(min_value=1, max_value=365)

class ForecastCreateResponseSerializer(serializers.Serializer):
    forecastJobId = serializers.CharField()
    status = serializers.CharField()

class ForecastJobSerializer(serializers.Serializer):
    forecastJobId = serializers.CharField()
    status = serializers.CharField()
    modelType = serializers.CharField()
    horizon = serializers.IntegerField()
    createdAt = serializers.CharField()
    startedAt = serializers.CharField(allow_null=True)
    finishedAt = serializers.CharField(allow_null=True)
    outputUri = serializers.CharField(allow_null=True)
    errorMessage = serializers.CharField(allow_null=True)

class ForecastResultSerializer(serializers.Serializer):
    predictions = serializers.ListField(child=serializers.DictField())
    metrics = serializers.DictField()
    modelArtifactVersion = serializers.CharField()
