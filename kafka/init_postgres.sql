-- Initialize PostgreSQL with sensor metadata for CDC
-- This script creates the sensors table for Debezium CDC

-- Create database if not exists
CREATE DATABASE IF NOT EXISTS traffic_platform;

-- Use the database
\c traffic_platform;

-- Create schema if not exists
CREATE SCHEMA IF NOT EXISTS public;

-- Drop existing table to start fresh
DROP TABLE IF EXISTS public.sensors CASCADE;

-- Create sensors table
CREATE TABLE public.sensors (
    sensor_id VARCHAR(50) PRIMARY KEY,
    road_name VARCHAR(100) NOT NULL,
    city VARCHAR(50) NOT NULL,
    direction VARCHAR(10) NOT NULL,
    latitude DECIMAL(10, 6) NOT NULL,
    longitude DECIMAL(10, 6) NOT NULL,
    installed_date TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP,
    is_active BOOLEAN DEFAULT TRUE,
    updated_at TIMESTAMP WITH TIME ZONE DEFAULT CURRENT_TIMESTAMP
);

-- Create indexes for CDC
CREATE INDEX IF NOT EXISTS idx_sensors_updated_at ON public.sensors(updated_at);
CREATE INDEX IF NOT EXISTS idx_sensors_is_active ON public.sensors(is_active);

-- Insert sample sensor data (10 sensors along I-94 corridor)
INSERT INTO public.sensors (sensor_id, road_name, city, direction, latitude, longitude, is_active) VALUES
('i94_sensor_01', 'I-94 Westbound', 'Minneapolis', 'West', 44.98, -93.27, TRUE),
('i94_sensor_02', 'I-94 Westbound', 'St. Paul', 'West', 44.95, -93.10, TRUE),
('i94_sensor_03', 'I-94 Westbound', 'Minneapolis', 'West', 44.97, -93.25, TRUE),
('i94_sensor_04', 'I-94 Westbound', 'Minneapolis', 'West', 44.99, -93.23, TRUE),
('i94_sensor_05', 'I-94 Westbound', 'St. Paul', 'West', 44.94, -93.05, TRUE),
('i94_sensor_06', 'I-94 Westbound', 'Minneapolis', 'West', 45.01, -93.20, TRUE),
('i94_sensor_07', 'I-94 Westbound', 'St. Paul', 'West', 45.02, -93.15, TRUE),
('i94_sensor_08', 'I-94 Westbound', 'Minneapolis', 'West', 45.03, -93.18, TRUE),
('i94_sensor_09', 'I-94 Westbound', 'St. Paul', 'West', 45.00, -93.12, TRUE),
('i94_sensor_10', 'I-94 Westbound', 'Minneapolis', 'West', 44.96, -93.22, TRUE);

-- Grant permissions for Debezium
GRANT SELECT, INSERT, UPDATE, DELETE ON public.sensors TO debezium_user;
GRANT USAGE ON SCHEMA public TO debezium_user;

-- Create publication for CDC
CREATE PUBLICATION IF NOT EXISTS debezium_publication FOR TABLE public.sensors;

-- Log initialization
DO $$
BEGIN
    RAISE NOTICE 'PostgreSQL initialized with % sensors', (SELECT COUNT(*) FROM public.sensors);
END $$;
