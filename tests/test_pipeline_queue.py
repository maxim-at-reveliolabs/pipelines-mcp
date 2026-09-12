from pipelines_mcp.models import PipelineQueueItem


def test_queue_item_fields_are_request_id_status_queue_tag() -> None:
    item = PipelineQueueItem(
        request_id="r1",
        status="pending",
        queue_tag="high",
    )
    assert item.model_dump() == {
        "request_id": "r1",
        "status": "pending",
        "queue_tag": "high",
    }
