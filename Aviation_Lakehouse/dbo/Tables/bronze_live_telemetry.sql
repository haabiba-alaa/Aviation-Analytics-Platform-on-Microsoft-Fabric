CREATE TABLE [dbo].[bronze_live_telemetry] (
    [timestamp]             VARCHAR (8000) NULL,
    [flight_number]         BIGINT         NULL,
    [airline]               VARCHAR (8000) NULL,
    [origin]                VARCHAR (8000) NULL,
    [destination]           VARCHAR (8000) NULL,
    [current_altitude_ft]   BIGINT         NULL,
    [speed_knots]           BIGINT         NULL,
    [departure_delay_mins]  BIGINT         NULL,
    [event_type]            VARCHAR (8000) NULL,
    [EventProcessedUtcTime] DATETIME2 (6)  NULL,
    [PartitionId]           BIGINT         NULL,
    [EventEnqueuedUtcTime]  DATETIME2 (6)  NULL
);


GO