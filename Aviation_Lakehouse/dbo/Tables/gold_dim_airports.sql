CREATE TABLE [dbo].[gold_dim_airports] (
    [airport_code] VARCHAR (8000) NULL,
    [airport_name] VARCHAR (8000) NULL,
    [city]         VARCHAR (8000) NULL,
    [state]        VARCHAR (8000) NULL,
    [country]      VARCHAR (8000) NULL,
    [latitude]     FLOAT (53)     NULL,
    [longitude]    FLOAT (53)     NULL
);


GO