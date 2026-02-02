from rest_framework import serializers

class ForecastCreateSerializer(serializers.Serializer):
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
