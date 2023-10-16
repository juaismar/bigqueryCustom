package test

import (
	"github.com/sirupsen/logrus"
	"github.com/stretchr/testify/suite"
	"github.com/juaismar/bigqueryCustom"
	"gorm.io/gorm"
	"log"
)

type GormTestSuite struct {
	suite.Suite
	db *gorm.DB
}

func (suite *GormTestSuite) SetupSuite() {

	logrus.SetLevel(logrus.DebugLevel)

	var err error
	suite.db, err = gorm.Open(bigquery.Open("bigquery://go-bigquery-driver/playground"), &gorm.Config{})
	if err != nil {
		log.Fatal(err)
	}
}

func (suite *GormTestSuite) TearDownSuite() {

}
