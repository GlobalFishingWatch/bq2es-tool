package utils

import (
	"log"
	"regexp"
)

func ValidateUrl(url string) {
	re := regexp.MustCompile(`^http://(.*)|^https://(.*)`)
	match := re.Match([]byte(url))
	if !match {
		log.Fatalln("--elastic-search-url should start with 'http://' or 'https://'")
	}
}
