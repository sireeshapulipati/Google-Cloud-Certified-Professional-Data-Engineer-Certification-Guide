from google.cloud import monitoring_v3
from google.protobuf.timestamp_pb2 import Timestamp 
import time 

 
client = monitoring_v3.MetricServiceClient() 
project_name = f"projects/your-project-id" 
 
descriptor = monitoring_v3.MetricDescriptor() 
descriptor.type = "custom.googleapis.com/dataflow/malformed_record_count" 
descriptor.metric_kind = monitoring_v3.MetricDescriptor.MetricKind.CUMULATIVE 
descriptor.value_type = monitoring_v3.MetricDescriptor.ValueType.INT64 
descriptor.description = "Tracks number of malformed records" 
descriptor.labels.append( 
    monitoring_v3.LabelDescriptor( 
        key="step_name", 
        value_type=monitoring_v3.LabelDescriptor.ValueType.STRING, 
        description="The pipeline step name" 
    ) 
) 
client.create_metric_descriptor(name=project_name, metric_descriptor=descriptor) 

series = monitoring_v3.TimeSeries() 
series.metric.type = "custom.googleapis.com/dataflow/malformed_record_count" 
series.resource.type = "global" 
series.metric.labels["step_name"] = "validate_record" 
 
point = monitoring_v3.Point() 
point.value.int64_value = 1 
now = time.time() 
timestamp = Timestamp() 
timestamp.FromSeconds(int(now)) 
point.interval.end_time = timestamp 
series.points.append(point) 
 
client.create_time_series(name=project_name, time_series=[series]) 