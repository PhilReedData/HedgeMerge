CREATE TABLE Return
(
  Source CHAR(1) NOT NULL,
  SourceFundID VARCHAR(50) NOT NULL,
  1994-01 INT,
  1994-02 FLOAT,
  ... FLOAT NOT NULL,
  2016-12 FLOAT,
  2016-11 FLOAT,
  PRIMARY KEY (Source, SourceFundID)
);

CREATE TABLE AUM
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

CREATE TABLE SourceCharacteristics
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

CREATE TABLE MergedCharacteristics
(
  Source INT NOT NULL,
  SourceFundID INT NOT NULL,
  FundName INT NOT NULL,
  Currency INT NOT NULL,
  CompanyName INT NOT NULL,
  StdCompanyName INT,
  LongestHist INT,
  CompanyID INT NOT NULL,
  ManagementFee INT NOT NULL,
  IncentiveFee INT NOT NULL,
  LockUp INT NOT NULL,
  Notice VARCHAR NOT NULL,
  HWM INT NOT NULL,
  Leverage INT NOT NULL,
  MinimumInvestment INT NOT NULL,
  RedemptionFrequency VARCHAR NOT NULL,
  SubscriptionFrequency VARCHAR NOT NULL,
  Strategy VARCHAR NOT NULL,
  Domicile VARCHAR NOT NULL,
  Closed INT NOT NULL,
  Liquidated INT NOT NULL,
  MergedFundID INT,
  PRIMARY KEY (Source, SourceFundID)
);

CREATE TABLE TASSCharacteristics
(
  Source CHAR(1) NOT NULL,
  SourceFundID VARCHAR(50) NOT NULL,
  TASS_ID1 VARCHAR,
  TASS_ID2 VARCHAR,
  TASS_ID... VARCHAR,
  PRIMARY KEY (Source, SourceFundID)
);

CREATE TABLE EurekaCharacteristics
(
  Source CHAR(1) NOT NULL,
  SourceFundID VARCHAR(50) NOT NULL,
  Eureka_ID1 VARCHAR,
  Eureka_ID2 VARCHAR,
  Eureka_ID... VARCHAR,
  PRIMARY KEY (Source, SourceFundID)
);
