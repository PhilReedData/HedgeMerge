CREATE TABLE IF NOT EXISTS RateOfReturn
(
  Source CHAR(1) NOT NULL,
  SourceFundID VARCHAR(50) NOT NULL,
  1994-01 INT,
  1994-02 FLOAT,
  ... FLOAT,
  2016-12 FLOAT,
  2016-11 FLOAT,
  PRIMARY KEY (Source, SourceFundID)
);

CREATE TABLE IF NOT EXISTS AUM
(
  Source CHAR(1) NOT NULL,
  SourceFundID VARCHAR(50) NOT NULL,
  1994-01 FLOAT,
  1994-02 FLOAT,
  ... FLOAT NOT NULL,
  2016-12 FLOAT,
  2016-11 FLOAT,
  PRIMARY KEY (Source, SourceFundID)
);

CREATE TABLE IF NOT EXISTS SourceCharacteristics
(
  Source CHAR(1) NOT NULL,
  SourceFundID VARCHAR(50) NOT NULL,
  TASS_ID1 VARCHAR,
  TASS_ID2 VARCHAR,
  Eureka_ID1 VARCHAR,
  Eureka_ID2 VARCHAR,
  TASS_ID... VARCHAR,
  Eureka_ID... VARCHAR,
  PRIMARY KEY (Source, SourceFundID)
);

CREATE TABLE IF NOT EXISTS MergedCharacteristics
(
  Source INT NOT NULL,
  SourceFundID INT NOT NULL,
  FundName INT NOT NULL,
  Currency INT NOT NULL,
  CompanyName INT NOT NULL,
  StdCompanyName INT,
  UseDummy INT,
  PRIMARY KEY (Source, SourceFundID)
);

CREATE TABLE IF NOT EXISTS TASSCharacteristics
(
  Source CHAR(1) NOT NULL,
  SourceFundID VARCHAR(50) NOT NULL,
  TASS_ID1 VARCHAR,
  TASS_ID2 VARCHAR,
  TASS_ID... VARCHAR,
  PRIMARY KEY (Source, SourceFundID)
);

CREATE TABLE IF NOT EXISTS EurekaCharacteristics
(
  Source CHAR(1) NOT NULL,
  SourceFundID VARCHAR(50) NOT NULL,
  Eureka_ID1 VARCHAR,
  Eureka_ID2 VARCHAR,
  Eureka_ID... VARCHAR,
  PRIMARY KEY (Source, SourceFundID)
);
