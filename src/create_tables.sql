-- DROP DATABASE PropertyPrice;
CREATE DATABASE IF NOT EXISTS PropertyPrice;
USE PropertyPrice;

CREATE TABLE `District` (
    -- shld not be auto_increment, pre-defined
    `district_id` int NOT NULL ,
    `district_name` char NOT NULL ,
    `coordinates` char ,
    PRIMARY KEY (
        `district_id`
    )
);

CREATE TABLE `Project` (
    `project_id` int AUTO_INCREMENT NOT NULL ,
    `district_id` int  NOT NULL ,
    `project_name` char  NOT NULL ,
    `long` float,
    `lat` float,
    PRIMARY KEY (
        `project_id`
    ),
    UNIQUE KEY (project_name),
    -- if lat and long gotten from new python ver is diff, then update, does not require unique
    CONSTRAINT `fk_Project_district_id` FOREIGN KEY(`district_id`) REFERENCES `District` (`district_id`)
);

CREATE TABLE `Property` (
    -- AUTO_INCREMENT?
    `property_id` int NOT NULL ,
    `project_id` int  NOT NULL ,
    `property_type` int  NOT NULL ,
    `street` varchar(256),
    -- TODO discussion street: a broader definition than long / lat / district_id in Project?
    `lease_year` smallint,
    `lease_duration` smallint,
    `floor_range_start` tinyint,
    `floor_range_end` tinyint,
    `floor_area` float,
    PRIMARY KEY (
        `property_id`
    ),
    CONSTRAINT `fk_Property_project_id` FOREIGN KEY(`project_id`) REFERENCES `Project` (`project_id`)
);

CREATE TABLE `Transaction` (
    `transaction_id` int AUTO_INCREMENT NOT NULL ,
    `property_id` int  NOT NULL ,
    `transaction_year` smallint  NOT NULL ,
    `transaction_month` tinyint  NOT NULL ,
    `type_of_sale` tinyint  NOT NULL ,
    `price` int  NOT NULL ,
    PRIMARY KEY (
        `transaction_id`
    ),
	CONSTRAINT `fk_Transaction_property_id`  FOREIGN KEY(`property_id`) REFERENCES `Property` (`property_id`)
);

CREATE TABLE `Amenities` (
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


