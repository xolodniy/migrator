package main

import (
	"embed"
	"fmt"
	"log"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/andreyvit/diff"
	"github.com/gin-gonic/gin/binding"
	"github.com/sirupsen/logrus"
	"gopkg.in/yaml.v2"
	"gorm.io/driver/postgres"
	"gorm.io/gorm"
	"gorm.io/gorm/logger"
)

//go:embed "migrations/*.sql"
var Embed embed.FS

const migrationsDirName = "migrations"

type Config struct {
	LogLevel string `yaml:"logLevel" binding:"required"`
	Database struct {
		Name     string `yaml:"name"     binding:"required"`
		Host     string `yaml:"host"     binding:"required"`
		Port     int    `yaml:"port"     binding:"min=1,max=65535"`
		User     string `yaml:"user"     binding:"required"`
		Password string `yaml:"password" binding:"required"`
	}
}

// ConnURL returns string URL, which may be used for connect to postgres database.
func (c *Config) ConnURL() string {
	url := fmt.Sprintf(
		"postgres://%s:%s@%s:%d/%s",
		c.Database.User,
		c.Database.Password,
		c.Database.Host,
		c.Database.Port,
		c.Database.Name,
	)
	return url
}

func initConfig(path string) Config {
	file, err := os.Open(path)
	if err != nil {
		logrus.WithError(err).WithField("path", path).Fatal("can't read config file")
	}
	var config Config
	// Init new YAML decode
	d := yaml.NewDecoder(file)
	// Start YAML decoding from file
	if err := d.Decode(&config); err != nil {
		logrus.WithError(err).Fatal("can't decode config file")
	}

	if err := binding.Validator.ValidateStruct(config); err != nil {
		logrus.WithError(err).Fatal("config validation failed")
	}

	level, err := logrus.ParseLevel(config.LogLevel)
	if err != nil {
		logrus.Fatal("invalid 'logLevel' parameter in configuration. Available values: ", logrus.AllLevels)
	}
	logrus.SetLevel(level)
	logrus.SetReportCaller(true) // adds line number to log message
	logrus.SetFormatter(&logrus.TextFormatter{ForceColors: true})

	return config
}

type Migration struct {
	ID        int
	CreatedAt time.Time
	Name      string
	Body      string
}

func main() {
	config := initConfig("./config.example.yaml")

	db, err := gorm.Open(postgres.Open(config.ConnURL()), &gorm.Config{
		Logger: logger.New(
			log.New(os.Stderr, "\r\n", log.LstdFlags), // io writer
			logger.Config{
				SlowThreshold:             time.Second / 5, // Slow SQL threshold
				LogLevel:                  logger.Silent,   // Log level
				IgnoreRecordNotFoundError: true,
				Colorful:                  true,
			},
		),
	})
	if err != nil {
		logrus.Fatal(err)
	}
	if !db.Migrator().HasTable(&Migration{}) {
		if err := db.Migrator().CreateTable(&Migration{}); err != nil {
			logrus.Fatal(err)
		}
	}

	var applied []Migration
	if err := db.Order("name").Find(&applied).Error; err != nil {
		logrus.Fatal(err)
	}
	dir, err := Embed.ReadDir(migrationsDirName)
	if err != nil {
		logrus.WithError(err).Fatal("can't read migrations dir")
	}
	sort.Slice(dir, func(i, j int) bool {
		return dir[i].Name() < dir[j].Name()
	})
	var files = make([]string, len(dir))
	for i := range dir {
		file, err := Embed.ReadFile(migrationsDirName + "/" + dir[i].Name())
		if err != nil {
			logrus.WithError(err).WithField("filename", dir[i].Name()).Fatal("can't read migration file")
		}
		files[i] = string(file)
	}
	logrus.SetFormatter(&logrus.TextFormatter{DisableQuote: true})
	for i := range applied {
		if len(files) <= i {
			logrus.Fatalf("migration %s was removed", applied[i].Name)
		}

		// Support multi-platform line-separator
		applied[i].Body = strings.Replace(applied[i].Body, "\r\n", "\r", -1)
		files[i] = strings.Replace(files[i], "\r\n", "\r", -1)
		if files[i] != applied[i].Body {
			logrus.
				WithField("diff", diff.CharacterDiff(applied[i].Body, files[i])).
				Fatalf("migration %s was changed", applied[i].Name)
		}
	}

	// Trim from box migrations whose already applied
	files = files[len(applied):]
	dir = dir[len(applied):]
	if len(files) == 0 {
		fmt.Println("Found no one new migration, your database is up to date.")
		return
	}

	// Next migrations expected as new and will be incremental applied now
	for i := range files {
		tx := db.Begin()
		if err := tx.Create(&Migration{Name: dir[i].Name(), Body: files[i]}).Error; err != nil {
			tx.Rollback()
			logrus.WithError(err).Fatal("can't init migration stat")
		}
		if err := tx.Exec(files[i]).Error; err != nil {
			tx.Rollback()
			logrus.WithError(err).Fatalf("can't execute migration %s", dir[i].Name())
		}
		if err := tx.Commit().Error; err != nil {
			logrus.WithError(err).Fatal("can't commit transaction")
		}
	}

	fmt.Println("Has applied migrations:")
	for i := range dir {
		fmt.Println(" - ", dir[i].Name())
	}
}
