from django.conf import settings
from eta.external_task_scheduler import EcsTaskExecutor, DatadogSidecarConfig
from infra.ec2_metadata import EC2Metadata
from infra.ecs_container_metadata import ECSContainerMetadata

def get_executor(container_metadata=None, cluster=None):
    subnet_ids = EC2Metadata.subnet_ids()
    security_group_ids = EC2Metadata.security_group_ids()

    ecs_container_metadata = None
    if settings.ECS_CONTAINER_METADATA_FILE:
        ecs_container_metadata = ECSContainerMetadata(settings.ECS_CONTAINER_METADATA_FILE)
    elif container_metadata:
        ecs_container_metadata = ECSContainerMetadata(container_metadata)

    ecs_task_executor = EcsTaskExecutor(
        ecs_cluster=cluster if cluster else settings.EXTERNAL_ETA_ECS_CLUSTER,
        source_task=ecs_container_metadata.task_id,
        task_definition_family=ecs_container_metadata.task_definition_family,
        task_definition_revision=ecs_container_metadata.task_definition_revision,
        container_name=ecs_container_metadata.container_name,
        subnet_ids=subnet_ids,
        securtiy_group_ids=security_group_ids,
        datadog_sidecar_config=DatadogSidecarConfig(
            sidecar_image=settings.EXTERNAL_ETA_DATADOG_SIDECAR_IMAGE,
            api_key=settings.DATADOG_API_KEY,
            environment=settings.ENVIRONMENT.lower(),
        )
    )
    return ecs_task_executor
