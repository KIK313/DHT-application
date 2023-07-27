package main

import (
	"bufio"
	"fmt"
	"os"
	"strings"
	"time"

	"github.com/fatih/color"
)

var (
	yellow = color.New(color.FgYellow)
)

func gettext(s *string) {
	reader := bufio.NewReader(os.Stdin)
	text, _ := reader.ReadString('\n')
	text = strings.Replace(text, "\n", "", -1)
	*s = text
}
func main() {
	//	fmt.Printf("%c[3;40;33mWelcome to P2P Chat!\n%c[0m", 0x1B, 0x1B)
	yellow.Printf("Welcome to P2P Chat!\n\n")
	yellow.Printf("Please type your ip:\t")
	var IP string
	gettext(&IP)
	yellow.Printf("%s \n", IP)
	yellow.Printf("Please type your nickname:\t")
	var nickname string
	gettext(&nickname)
	yellow.Printf("Do You Want to Create a P2P network? \n [yes] or [no]\t")
	var yn string
	gettext(&yn)
	var in string
	node := NewNode(IP)
	node.Run()
	time.Sleep(300 * time.Millisecond)
	defer node.Quit()
	if strings.Compare(yn, "yes") == 0 {

		node.Create()

	} else {
		yellow.Printf("Please tell me your introducer\t")
		gettext(&in)
		node.Join(in)
	}
	yellow.Printf("Now You Can Chat Freely\n")
	_, ss := node.Get("1")
	yellow.Printf("%s", ss)
	for {
		yellow.Printf(">>")
		var e string
		gettext(&e)
		if e == "" {
			_, ss := node.Get("1")
			yellow.Printf("%s\n", ss)
		} else {
			if e == "exit" {
				break
			} else {
				d := fmt.Sprintf("[%s]: %s %s\n", nickname, e, time.Now().Format("2006-01-02 15:04:05"))
				node.Put("1", d)
			}
		}
	}
}
