
CREATE TABLE IF NOT EXISTS `District` (
    `id` int AUTO_INCREMENT NOT NULL,
    `district_name` varchar(50) NOT NULL ,
    `coordinates` LONGTEXT,
    PRIMARY KEY (
        `id`
    ),
    UNIQUE KEY (district_name)
);

CREATE TABLE IF NOT EXISTS `Project` (
    `id` int AUTO_INCREMENT NOT NULL ,
    `district_id` int  NOT NULL ,
    `project_name` varchar(100) NOT NULL,
    `address` varchar(100) NOT NULL,
    `long` DOUBLE,
    `lat` DOUBLE,
    PRIMARY KEY (
        `id`
    ),
    UNIQUE KEY (`address`, project_name, `long`, lat),
    CONSTRAINT `fk_Project_district_id` FOREIGN KEY(`district_id`) REFERENCES `District` (`id`)
);

CREATE TABLE IF NOT EXISTS `Property` (
    `id` int AUTO_INCREMENT NOT NULL ,
    `project_id` int  NOT NULL ,
    `property_type` varchar(25),
    `lease_year` smallint,
    `lease_duration` int,
    `floor_range_start` tinyint,
    `floor_range_end` tinyint,
    `floor_area` float,
    PRIMARY KEY (
        `id`
    ),
    CONSTRAINT `fk_Property_project_id` FOREIGN KEY(`project_id`) REFERENCES `Project` (`id`)
);

CREATE TABLE IF NOT EXISTS `Transaction` (
    `id` int AUTO_INCREMENT NOT NULL ,
    `property_id` int  NOT NULL ,
    `transaction_year` smallint  NOT NULL ,
    `transaction_month` tinyint  NOT NULL ,
    `type_of_sale` varchar(15)  NOT NULL ,
    `price` float  NOT NULL ,
    PRIMARY KEY (
        `id`
    ),
	CONSTRAINT `fk_Transaction_property_id`  FOREIGN KEY(`property_id`) REFERENCES `Property` (`id`)
);

CREATE TABLE IF NOT EXISTS `Amenity` (
    `id` int AUTO_INCREMENT NOT NULL ,
    `district_id` int  NOT NULL ,
    `amenity_type` varchar(100)  NOT NULL ,
    `long` DOUBLE,
    `lat` DOUBLE,
    PRIMARY KEY (
        `id`
    ),
    UNIQUE KEY (`amenity_type`, `long`, lat),
    CONSTRAINT `fk_Amenities_district_id` FOREIGN KEY(`district_id`) REFERENCES `District` (`id`)
);