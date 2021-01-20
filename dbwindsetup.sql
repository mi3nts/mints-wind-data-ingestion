/*
  SQL setup script for the PostgreSQL database to store wind data on
*/

CREATE TABLE public.wind_data (
    recorded_time timestamp with time zone,
    header jsonb,
    data jsonb
);
