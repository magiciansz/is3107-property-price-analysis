CREATE DATABASE Property;

CREATE TABLE `Transaction` (
    `transaction_id` int AUTO_INCREMENT NOT NULL ,
    `property_id` int  NOT NULL ,
    `transaction_year` smallint  NOT NULL ,
    `transaction_month` tinyint  NOT NULL ,
    `type_of_sale` tinyint  NOT NULL ,
    `price` int  NOT NULL ,
    PRIMARY KEY (
        `transaction_id`
    )
);

CREATE TABLE `Property` (
    `property_id` int AUTO_INCREMENT NOT NULL ,
    `project_id` int  NOT NULL ,
    `property_type` int  NOT NULL ,
    `street` varchar  NOT NULL ,
    `lease_year` smallint  NOT NULL ,
    `lease_duration` smallint  NOT NULL ,
    `floor_range_start` tinyint  NOT NULL ,
    `floor_range_end` tinyint  NOT NULL ,
    `floor_area` float  NOT NULL ,
    PRIMARY KEY (
        `property_id`
    )
);

CREATE TABLE `Project` (
    `project_id` int AUTO_INCREMENT NOT NULL ,
    `district_id` int  NOT NULL ,
    `project_name` string  NOT NULL ,
    `long` float  NOT NULL ,
    `lat` float  NOT NULL ,
    PRIMARY KEY (
        `project_id`
    )
);

CREATE TABLE `District` (
    `district_id` int AUTO_INCREMENT NOT NULL ,
    `district_name` string  NOT NULL ,
    `coordinates` string  NOT NULL ,
    PRIMARY KEY (
        `district_id`
    )
);

CREATE TABLE `Amenities` (
    `amenity_id` int AUTO_INCREMENT NOT NULL ,
    `district_id` int  NOT NULL ,
    `amenity_type` tinyint  NOT NULL ,
    `long` float  NOT NULL ,
    `lat` float  NOT NULL ,
    PRIMARY KEY (
        `amenity_id`
    )
);

ALTER TABLE `Transaction` ADD CONSTRAINT `fk_Transaction_property_id` FOREIGN KEY(`property_id`)
REFERENCES `Property` (`property_id`);

ALTER TABLE `Property` ADD CONSTRAINT `fk_Property_project_id` FOREIGN KEY(`project_id`)
REFERENCES `Project` (`project_id`);

ALTER TABLE `Project` ADD CONSTRAINT `fk_Project_district_id` FOREIGN KEY(`district_id`)
REFERENCES `District` (`district_id`);

ALTER TABLE `Amenities` ADD CONSTRAINT `fk_Amenities_district_id` FOREIGN KEY(`district_id`)
REFERENCES `District` (`district_id`);

