-- DROP DATABASE PropertyPrice;

-- SELECT CONCAT('DROP TABLE IF EXISTS `', table_name, '`;')
-- FROM information_schema.tables
-- WHERE table_schema = 'propertyprice';

-- SET FOREIGN_KEY_CHECKS = 0;
-- DROP TABLE IF EXISTS `amenities`;
-- DROP TABLE IF EXISTS `district`;
-- DROP TABLE IF EXISTS `project`;
-- DROP TABLE IF EXISTS `property`;
-- DROP TABLE IF EXISTS `transaction`;


CREATE DATABASE IF NOT EXISTS PropertyPrice;
USE PropertyPrice;

CREATE TABLE IF NOT EXISTS `District` (
    `district_id` int NOT NULL ,
    `district_name` varchar(50) NOT NULL ,
    -- update coordinate datatype when district data is avail
    `coordinates` varchar(255) ,
    PRIMARY KEY (
        `district_id`
    )
);

CREATE TABLE IF NOT EXISTS `Project` (
    `project_id` int AUTO_INCREMENT NOT NULL ,
    `district_id` int  NOT NULL ,
    `project_name` varchar(100) NOT NULL ,
    `long` float,
    `lat` float,
    PRIMARY KEY (
        `project_id`
    ),
    UNIQUE KEY (project_name),
    -- if lat and long gotten from new python ver is diff, then update, does not require unique
    CONSTRAINT `fk_Project_district_id` FOREIGN KEY(`district_id`) REFERENCES `District` (`district_id`)
);

CREATE TABLE IF NOT EXISTS `Property` (
    -- AUTO_INCREMENT?
    `property_id` int AUTO_INCREMENT NOT NULL ,
    `project_id` int  NOT NULL ,
    `property_type` varchar(25),
    `street` varchar(100),
    -- TODO discussion street: a broader definition than long / lat / district_id in Project?
    `lease_year` smallint,
    `lease_duration` smallint,
    `floor_range_start` tinyint,
    `floor_range_end` tinyint,
    `floor_area` float,
    PRIMARY KEY (
        `property_id`
    ),
    UNIQUE KEY (project_id, property_type, street, lease_year, lease_duration, floor_range_start, floor_range_end, floor_area),
    CONSTRAINT `fk_Property_project_id` FOREIGN KEY(`project_id`) REFERENCES `Project` (`project_id`)
);

CREATE TABLE IF NOT EXISTS `Transactions` (
    `transaction_id` int AUTO_INCREMENT NOT NULL ,
    `property_id` int  NOT NULL ,
    `transaction_year` smallint  NOT NULL ,
    `transaction_month` tinyint  NOT NULL ,
    `type_of_sale` varchar(15)  NOT NULL ,
    -- can discuss the efficiency issue of storing varchar vs char
    `price` float  NOT NULL ,
    PRIMARY KEY (
        `transaction_id`
    ),
	CONSTRAINT `fk_Transaction_property_id`  FOREIGN KEY(`property_id`) REFERENCES `Property` (`property_id`)
);

CREATE TABLE IF NOT EXISTS `Amenities` (
    `amenity_id` int AUTO_INCREMENT NOT NULL ,
    `district_id` int  NOT NULL ,
    `amenity_type` tinyint  NOT NULL ,
    `long` float,
    `lat` float,
    PRIMARY KEY (
        `amenity_id`
    ),
    CONSTRAINT `fk_Amenities_district_id` FOREIGN KEY(`district_id`) REFERENCES `District` (`district_id`)
);


