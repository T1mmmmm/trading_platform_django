from rest_framework import serializers


class LLMTaskCreateSerializer(serializers.Serializer):
    taskType = serializers.CharField()
    sourceType = serializers.CharField()
    sourceId = serializers.CharField()
    promptTemplateVersion = serializers.CharField(required=False, default="v1")


class LLMTaskCreateResponseSerializer(serializers.Serializer):
    llmTaskId = serializers.CharField()
    status = serializers.CharField()


class LLMTaskSerializer(serializers.Serializer):
    llmTaskId = serializers.CharField()
    taskType = serializers.CharField()
    sourceType = serializers.CharField()
    sourceId = serializers.CharField()
    status = serializers.CharField()
    modelName = serializers.CharField()
    outputUri = serializers.CharField(allow_null=True)
    errorMessage = serializers.CharField(allow_null=True)
    createdAt = serializers.CharField()
    startedAt = serializers.CharField(allow_null=True)
    finishedAt = serializers.CharField(allow_null=True)


class LLMTaskResultSerializer(serializers.Serializer):
    llmTaskId = serializers.CharField()
    reportId = serializers.CharField(allow_null=True)
    format = serializers.CharField()
    content = serializers.CharField()