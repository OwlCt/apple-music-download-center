package main

import (
	"bufio"
	"bytes"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"regexp"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"main/utils/ampapi"
	"main/utils/lyrics"
	"main/utils/runv2"
	"main/utils/runv3"
	"main/utils/structs"
	"main/utils/task"

	"github.com/AlecAivazis/survey/v2"
	"github.com/fatih/color"
	"github.com/gin-gonic/gin"
	"github.com/google/uuid"
	"github.com/grafov/m3u8"
	"github.com/olekukonko/tablewriter"
	"github.com/zhaarey/go-mp4tag"
	"gopkg.in/yaml.v2"
)

var (
	forbiddenNames = regexp.MustCompile(`[/\\<>:"|?*]`)
	dl_atmos       bool
	dl_aac         bool
	dl_select      bool
	dl_song        bool
	artist_select  bool
	debug_mode     bool
	alac_max       *int
	atmos_max      *int
	mv_max         *int
	mv_audio_type  *string
	aac_type       *string
	Config         structs.ConfigSet
	counter        structs.Counter
	okDict         = make(map[string][]int)
)

func loadConfig() error {
	data, err := os.ReadFile("config.yaml")
	if err != nil {
		return err
	}
	err = yaml.Unmarshal(data, &Config)
	if err != nil {
		return err
	}
	if len(Config.Storefront) != 2 {
		Config.Storefront = "us"
	}
	return nil
}

func LimitString(s string) string {
	if len([]rune(s)) > Config.LimitMax {
		return string([]rune(s)[:Config.LimitMax])
	}
	return s
}

func isInArray(arr []int, target int) bool {
	for _, num := range arr {
		if num == target {
			return true
		}
	}
	return false
}

func fileExists(path string) (bool, error) {
	f, err := os.Stat(path)
	if err == nil {
		return !f.IsDir(), nil
	} else if os.IsNotExist(err) {
		return false, nil
	}
	return false, err
}

func checkUrl(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/album|\/album\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlMv(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music)\.apple\.com\/(\w{2})(?:\/music-video|\/music-video\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlSong(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/song|\/song\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func checkUrlPlaylist(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/playlist|\/playlist\/.+))\/(?:id)?(pl\.[\w-]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}

func checkUrlStation(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music)\.apple\.com\/(\w{2})(?:\/station|\/station\/.+))\/(?:id)?(ra\.[\w-]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}

func checkUrlArtist(url string) (string, string) {
	pat := regexp.MustCompile(`^(?:https:\/\/(?:beta\.music|music|classical\.music)\.apple\.com\/(\w{2})(?:\/artist|\/artist\/.+))\/(?:id)?(\d[^\D]+)(?:$|\?)`)
	matches := pat.FindAllStringSubmatch(url, -1)

	if matches == nil {
		return "", ""
	} else {
		return matches[0][1], matches[0][2]
	}
}
func getUrlSong(songUrl string, token string) (string, error) {
	storefront, songId := checkUrlSong(songUrl)
	manifest, err := ampapi.GetSongResp(storefront, songId, Config.Language, token)
	if err != nil {
		fmt.Println("\u26A0 Failed to get manifest:", err)
		counter.NotSong++
		return "", err
	}
	albumId := manifest.Data[0].Relationships.Albums.Data[0].ID
	songAlbumUrl := fmt.Sprintf("https://music.apple.com/%s/album/1/%s?i=%s", storefront, albumId, songId)
	return songAlbumUrl, nil
}
func getUrlArtistName(artistUrl string, token string) (string, string, error) {
	storefront, artistId := checkUrlArtist(artistUrl)
	req, err := http.NewRequest("GET", fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s", storefront, artistId), nil)
	if err != nil {
		return "", "", err
	}
	req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	req.Header.Set("Origin", "https://music.apple.com")
	query := url.Values{}
	query.Set("l", Config.Language)
	do, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", "", err
	}
	defer do.Body.Close()
	if do.StatusCode != http.StatusOK {
		return "", "", errors.New(do.Status)
	}
	obj := new(structs.AutoGeneratedArtist)
	err = json.NewDecoder(do.Body).Decode(&obj)
	if err != nil {
		return "", "", err
	}
	return obj.Data[0].Attributes.Name, obj.Data[0].ID, nil
}

func checkArtist(artistUrl string, token string, relationship string) ([]string, error) {
	storefront, artistId := checkUrlArtist(artistUrl)
	Num := 0
	//id := 1
	var args []string
	var urls []string
	var options [][]string
	for {
		req, err := http.NewRequest("GET", fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s/%s?limit=100&offset=%d&l=%s", storefront, artistId, relationship, Num, Config.Language), nil)
		if err != nil {
			return nil, err
		}
		req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
		req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
		req.Header.Set("Origin", "https://music.apple.com")
		do, err := http.DefaultClient.Do(req)
		if err != nil {
			return nil, err
		}
		defer do.Body.Close()
		if do.StatusCode != http.StatusOK {
			return nil, errors.New(do.Status)
		}
		obj := new(structs.AutoGeneratedArtist)
		err = json.NewDecoder(do.Body).Decode(&obj)
		if err != nil {
			return nil, err
		}
		for _, album := range obj.Data {
			options = append(options, []string{album.Attributes.Name, album.Attributes.ReleaseDate, album.ID, album.Attributes.URL})
		}
		Num = Num + 100
		if len(obj.Next) == 0 {
			break
		}
	}
	sort.Slice(options, func(i, j int) bool {
		// 将日期字符串解析为 time.Time 类型进行比较
		dateI, _ := time.Parse("2006-01-02", options[i][1])
		dateJ, _ := time.Parse("2006-01-02", options[j][1])
		return dateI.Before(dateJ) // 返回 true 表示 i 在 j 前面
	})

	table := tablewriter.NewWriter(os.Stdout)
	if relationship == "albums" {
		table.SetHeader([]string{"", "Album Name", "Date", "Album ID"})
	} else if relationship == "music-videos" {
		table.SetHeader([]string{"", "MV Name", "Date", "MV ID"})
	}
	table.SetRowLine(false)
	table.SetHeaderColor(tablewriter.Colors{},
		tablewriter.Colors{tablewriter.FgRedColor, tablewriter.Bold},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor})

	table.SetColumnColor(tablewriter.Colors{tablewriter.FgCyanColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgRedColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor},
		tablewriter.Colors{tablewriter.Bold, tablewriter.FgBlackColor})
	for i, v := range options {
		urls = append(urls, v[3])
		options[i] = append([]string{fmt.Sprint(i + 1)}, v[:3]...)
		table.Append(options[i])
	}
	table.Render()
	if artist_select {
		fmt.Println("You have selected all options:")
		return urls, nil
	}
	reader := bufio.NewReader(os.Stdin)
	fmt.Println("Please select from the " + relationship + " options above (multiple options separated by commas, ranges supported, or type 'all' to select all)")
	cyanColor := color.New(color.FgCyan)
	cyanColor.Print("Enter your choice: ")
	input, _ := reader.ReadString('\n')

	input = strings.TrimSpace(input)
	if input == "all" {
		fmt.Println("You have selected all options:")
		return urls, nil
	}

	selectedOptions := [][]string{}
	parts := strings.Split(input, ",")
	for _, part := range parts {
		if strings.Contains(part, "-") {
			rangeParts := strings.Split(part, "-")
			selectedOptions = append(selectedOptions, rangeParts)
		} else {
			selectedOptions = append(selectedOptions, []string{part})
		}
	}

	fmt.Println("You have selected the following options:")
	for _, opt := range selectedOptions {
		if len(opt) == 1 {
			num, err := strconv.Atoi(opt[0])
			if err != nil {
				fmt.Println("Invalid option:", opt[0])
				continue
			}
			if num > 0 && num <= len(options) {
				fmt.Println(options[num-1])
				args = append(args, urls[num-1])
			} else {
				fmt.Println("Option out of range:", opt[0])
			}
		} else if len(opt) == 2 {
			start, err1 := strconv.Atoi(opt[0])
			end, err2 := strconv.Atoi(opt[1])
			if err1 != nil || err2 != nil {
				fmt.Println("Invalid range:", opt)
				continue
			}
			if start < 1 || end > len(options) || start > end {
				fmt.Println("Range out of range:", opt)
				continue
			}
			for i := start; i <= end; i++ {
				fmt.Println(options[i-1])
				args = append(args, urls[i-1])
			}
		} else {
			fmt.Println("Invalid option:", opt)
		}
	}
	return args, nil
}

func writeCover(sanAlbumFolder, name string, url string) (string, error) {
	covPath := filepath.Join(sanAlbumFolder, name+"."+Config.CoverFormat)
	if Config.CoverFormat == "original" {
		ext := strings.Split(url, "/")[len(strings.Split(url, "/"))-2]
		ext = ext[strings.LastIndex(ext, ".")+1:]
		covPath = filepath.Join(sanAlbumFolder, name+"."+ext)
	}
	exists, err := fileExists(covPath)
	if err != nil {
		fmt.Println("Failed to check if cover exists.")
		return "", err
	}
	if exists {
		_ = os.Remove(covPath)
	}
	if Config.CoverFormat == "png" {
		re := regexp.MustCompile(`\{w\}x\{h\}`)
		parts := re.Split(url, 2)
		url = parts[0] + "{w}x{h}" + strings.Replace(parts[1], ".jpg", ".png", 1)
	}
	url = strings.Replace(url, "{w}x{h}", Config.CoverSize, 1)
	if Config.CoverFormat == "original" {
		url = strings.Replace(url, "is1-ssl.mzstatic.com/image/thumb", "a5.mzstatic.com/us/r1000/0", 1)
		url = url[:strings.LastIndex(url, "/")]
	}
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return "", err
	}
	req.Header.Set("User-Agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/91.0.4472.124 Safari/537.36")
	do, err := http.DefaultClient.Do(req)
	if err != nil {
		return "", err
	}
	defer do.Body.Close()
	if do.StatusCode != http.StatusOK {
		return "", errors.New(do.Status)
	}
	f, err := os.Create(covPath)
	if err != nil {
		return "", err
	}
	defer f.Close()
	_, err = io.Copy(f, do.Body)
	if err != nil {
		return "", err
	}
	return covPath, nil
}

func writeLyrics(sanAlbumFolder, filename string, lrc string) error {
	lyricspath := filepath.Join(sanAlbumFolder, filename)
	f, err := os.Create(lyricspath)
	if err != nil {
		return err
	}
	defer f.Close()
	_, err = f.WriteString(lrc)
	if err != nil {
		return err
	}
	return nil
}

func contains(slice []string, item string) bool {
	for _, v := range slice {
		if v == item {
			return true
		}
	}
	return false
}

// START: New functions for search functionality

// SearchResultItem is a unified struct to hold search results for display.
type SearchResultItem struct {
	Type   string
	Name   string
	Detail string
	URL    string
	ID     string
}

// QualityOption holds information about a downloadable quality.
type QualityOption struct {
	ID          string
	Description string
}

// setDlFlags configures the global download flags based on the user's quality selection.
func setDlFlags(quality string) {
	dl_atmos = false
	dl_aac = false

	switch quality {
	case "atmos":
		dl_atmos = true
		fmt.Println("Quality set to: Dolby Atmos")
	case "aac":
		dl_aac = true
		*aac_type = "aac"
		fmt.Println("Quality set to: High-Quality (AAC)")
	case "alac":
		fmt.Println("Quality set to: Lossless (ALAC)")
	}
}

// promptForQuality asks the user to select a download quality for the chosen media.
func promptForQuality(item SearchResultItem, token string) (string, error) {
	if item.Type == "Artist" {
		fmt.Println("Artist selected. Proceeding to list all albums/videos.")
		return "default", nil
	}

	fmt.Printf("\nFetching available qualities for: %s\n", item.Name)

	qualities := []QualityOption{
		{ID: "alac", Description: "Lossless (ALAC)"},
		{ID: "aac", Description: "High-Quality (AAC)"},
		{ID: "atmos", Description: "Dolby Atmos"},
	}
	qualityOptions := []string{}
	for _, q := range qualities {
		qualityOptions = append(qualityOptions, q.Description)
	}

	prompt := &survey.Select{
		Message:  "Select a quality to download:",
		Options:  qualityOptions,
		PageSize: 5,
	}

	selectedIndex := 0
	err := survey.AskOne(prompt, &selectedIndex)
	if err != nil {
		// This can happen if the user presses Ctrl+C
		return "", nil
	}

	return qualities[selectedIndex].ID, nil
}

// handleSearch manages the entire interactive search process.
func handleSearch(searchType string, queryParts []string, token string) (string, error) {
	query := strings.Join(queryParts, " ")
	validTypes := map[string]bool{"album": true, "song": true, "artist": true}
	if !validTypes[searchType] {
		return "", fmt.Errorf("invalid search type: %s. Use 'album', 'song', or 'artist'", searchType)
	}

	fmt.Printf("Searching for %ss: \"%s\" in storefront \"%s\"\n", searchType, query, Config.Storefront)

	offset := 0
	limit := 15 // Increased limit for better navigation

	apiSearchType := searchType + "s"

	for {
		searchResp, err := ampapi.Search(Config.Storefront, query, apiSearchType, Config.Language, token, limit, offset)
		if err != nil {
			return "", fmt.Errorf("error fetching search results: %w", err)
		}

		var items []SearchResultItem
		var displayOptions []string
		hasNext := false

		// Special options for navigation
		const prevPageOpt = "⬅️  Previous Page"
		const nextPageOpt = "➡️  Next Page"

		// Add previous page option if applicable
		if offset > 0 {
			displayOptions = append(displayOptions, prevPageOpt)
		}

		switch searchType {
		case "album":
			if searchResp.Results.Albums != nil {
				for _, item := range searchResp.Results.Albums.Data {
					year := ""
					if len(item.Attributes.ReleaseDate) >= 4 {
						year = item.Attributes.ReleaseDate[:4]
					}
					trackInfo := fmt.Sprintf("%d tracks", item.Attributes.TrackCount)
					detail := fmt.Sprintf("%s (%s, %s)", item.Attributes.ArtistName, year, trackInfo)
					displayOptions = append(displayOptions, fmt.Sprintf("%s - %s", item.Attributes.Name, detail))
					items = append(items, SearchResultItem{Type: "Album", URL: item.Attributes.URL, ID: item.ID})
				}
				hasNext = searchResp.Results.Albums.Next != ""
			}
		case "song":
			if searchResp.Results.Songs != nil {
				for _, item := range searchResp.Results.Songs.Data {
					detail := fmt.Sprintf("%s (%s)", item.Attributes.ArtistName, item.Attributes.AlbumName)
					displayOptions = append(displayOptions, fmt.Sprintf("%s - %s", item.Attributes.Name, detail))
					items = append(items, SearchResultItem{Type: "Song", URL: item.Attributes.URL, ID: item.ID})
				}
				hasNext = searchResp.Results.Songs.Next != ""
			}
		case "artist":
			if searchResp.Results.Artists != nil {
				for _, item := range searchResp.Results.Artists.Data {
					detail := ""
					if len(item.Attributes.GenreNames) > 0 {
						detail = strings.Join(item.Attributes.GenreNames, ", ")
					}
					displayOptions = append(displayOptions, fmt.Sprintf("%s (%s)", item.Attributes.Name, detail))
					items = append(items, SearchResultItem{Type: "Artist", URL: item.Attributes.URL, ID: item.ID})
				}
				hasNext = searchResp.Results.Artists.Next != ""
			}
		}

		if len(items) == 0 && offset == 0 {
			fmt.Println("No results found.")
			return "", nil
		}

		// Add next page option if applicable
		if hasNext {
			displayOptions = append(displayOptions, nextPageOpt)
		}

		prompt := &survey.Select{
			Message:  "Use arrow keys to navigate, Enter to select:",
			Options:  displayOptions,
			PageSize: limit, // Show a full page of results
		}

		selectedIndex := 0
		err = survey.AskOne(prompt, &selectedIndex)
		if err != nil {
			// User pressed Ctrl+C
			return "", nil
		}

		selectedOption := displayOptions[selectedIndex]

		// Handle pagination
		if selectedOption == nextPageOpt {
			offset += limit
			continue
		}
		if selectedOption == prevPageOpt {
			offset -= limit
			continue
		}

		// Adjust index to match the `items` slice if "Previous Page" was an option
		itemIndex := selectedIndex
		if offset > 0 {
			itemIndex--
		}

		selectedItem := items[itemIndex]

		// Automatically set single song download flag
		if selectedItem.Type == "Song" {
			dl_song = true
		}

		quality, err := promptForQuality(selectedItem, token)
		if err != nil {
			return "", fmt.Errorf("could not process quality selection: %w", err)
		}
		if quality == "" { // User cancelled quality selection
			fmt.Println("Selection cancelled.")
			return "", nil
		}

		if quality != "default" {
			setDlFlags(quality)
		}

		return selectedItem.URL, nil
	}
}

// END: New functions for search functionality

func ripTrack(track *task.Track, token string, mediaUserToken string, onSub func(int, string)) {
	var err error
	counter.Total++
	fmt.Printf("Track %d of %d: %s\n", track.TaskNum, track.TaskTotal, track.Type)
	//提前获取到的播放列表下track所在的专辑信息
        if onSub != nil { onSub(0, track.Resp.Attributes.Name) }
        if onSub != nil { onSub(5, "") }
    if track.PreType == "playlists" && Config.UseSongInfoForPlaylist {
        track.GetAlbumData(token)
    }
	//mv dl dev
	if track.Type == "music-videos" {
		if len(mediaUserToken) <= 50 {
			fmt.Println("meida-user-token is not set, skip MV dl")
			counter.Success++
			return
		}
		if _, err := exec.LookPath("mp4decrypt"); err != nil {
			fmt.Println("mp4decrypt is not found, skip MV dl")
			counter.Success++
			return
		}
		err := mvDownloader(track.ID, track.SaveDir, token, track.Storefront, mediaUserToken, track)
		if err != nil {
			fmt.Println("\u26A0 Failed to dl MV:", err)
			counter.Error++
			return
		}
		counter.Success++
		return
	}
	needDlAacLc := false
	if dl_aac && Config.AacType == "aac-lc" {
		needDlAacLc = true
	}
	if track.WebM3u8 == "" && !needDlAacLc {
		if dl_atmos {
			fmt.Println("Unavailable")
			counter.Unavailable++
			return
		}
		fmt.Println("Unavailable, trying to dl aac-lc")
		needDlAacLc = true
	}
	needCheck := false

	if Config.GetM3u8Mode == "all" {
		needCheck = true
	} else if Config.GetM3u8Mode == "hires" && contains(track.Resp.Attributes.AudioTraits, "hi-res-lossless") {
		needCheck = true
	}
	var EnhancedHls_m3u8 string
	if needCheck && !needDlAacLc {
		EnhancedHls_m3u8, _ = checkM3u8(track.ID, "song")
		if strings.HasSuffix(EnhancedHls_m3u8, ".m3u8") {
			track.DeviceM3u8 = EnhancedHls_m3u8
			track.M3u8 = EnhancedHls_m3u8
		}
	}
	var Quality string
	if strings.Contains(Config.SongFileFormat, "Quality") {
		if dl_atmos {
			Quality = fmt.Sprintf("%dKbps", Config.AtmosMax-2000)
		} else if needDlAacLc {
			Quality = "256Kbps"
		} else {
			_, Quality, err = extractMedia(track.M3u8, true)
			if err != nil {
				fmt.Println("Failed to extract quality from manifest.\n", err)
				counter.Error++
				return
			}
		}
	}
	track.Quality = Quality

	stringsToJoin := []string{}
	if track.Resp.Attributes.IsAppleDigitalMaster {
		if Config.AppleMasterChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.AppleMasterChoice)
		}
	}
	if track.Resp.Attributes.ContentRating == "explicit" {
		if Config.ExplicitChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.ExplicitChoice)
		}
	}
	if track.Resp.Attributes.ContentRating == "clean" {
		if Config.CleanChoice != "" {
			stringsToJoin = append(stringsToJoin, Config.CleanChoice)
		}
	}
	Tag_string := strings.Join(stringsToJoin, " ")

	songName := strings.NewReplacer(
		"{SongId}", track.ID,
		"{SongNumer}", fmt.Sprintf("%02d", track.TaskNum),
		"{SongName}", LimitString(track.Resp.Attributes.Name),
		"{DiscNumber}", fmt.Sprintf("%0d", track.Resp.Attributes.DiscNumber),
		"{TrackNumber}", fmt.Sprintf("%0d", track.Resp.Attributes.TrackNumber),
		"{Quality}", Quality,
		"{Tag}", Tag_string,
		"{Codec}", track.Codec,
	).Replace(Config.SongFileFormat)
	fmt.Println(songName)
	filename := fmt.Sprintf("%s.m4a", forbiddenNames.ReplaceAllString(songName, "_"))
	track.SaveName = filename
	trackPath := filepath.Join(track.SaveDir, track.SaveName)
	lrcFilename := fmt.Sprintf("%s.%s", forbiddenNames.ReplaceAllString(songName, "_"), Config.LrcFormat)

	//get lrc
	var lrc string = ""
	if Config.EmbedLrc || Config.SaveLrcFile {
		lrcStr, err := lyrics.Get(track.Storefront, track.ID, Config.LrcType, Config.Language, Config.LrcFormat, token, mediaUserToken)
		if err != nil {
			fmt.Println(err)
		} else {
			if Config.SaveLrcFile {
				err := writeLyrics(track.SaveDir, lrcFilename, lrcStr)
				if err != nil {
					fmt.Printf("Failed to write lyrics")
				}
			}
			if Config.EmbedLrc {
				lrc = lrcStr
			}
		}
	}

	exists, err := fileExists(trackPath)
	if err != nil {
		fmt.Println("Failed to check if track exists.")
	}
	if exists {
		fmt.Println("Track already exists locally.")
		counter.Success++
		okDict[track.PreID] = append(okDict[track.PreID], track.TaskNum)
		return
	}
	if needDlAacLc {
		if len(mediaUserToken) <= 50 {
			fmt.Println("Invalid media-user-token")
			counter.Error++
			return
		}
        if onSub != nil { onSub(10, "") }
        _, err := runv3.Run(track.ID, trackPath, token, mediaUserToken, false)
        if err != nil {
            fmt.Println("Failed to dl aac-lc:", err)
            if err.Error() == "Unavailable" {
                counter.Unavailable++
                return
            }
            counter.Error++
            return
        }
        if onSub != nil { onSub(90, "") }
    } else {
        trackM3u8Url, _, err := extractMedia(track.M3u8, false)
        if err != nil {
            fmt.Println("\u26A0 Failed to extract info from manifest:", err)
            counter.Unavailable++
            return
        }
        if onSub != nil { onSub(10, "") }
        // 边下载边解密（无法精确进度，这里设置阶段性提示）
        err = runv2.Run(track.ID, trackM3u8Url, trackPath, Config)
        if err != nil {
            fmt.Println("Failed to run v2:", err)
            counter.Error++
            return
        }
        if onSub != nil { onSub(90, "") }
    }
	tags := []string{
		"tool=",
		fmt.Sprintf("artist=%s", track.Resp.Attributes.ArtistName),
	}
	if Config.EmbedCover {
		if (strings.Contains(track.PreID, "pl.") || strings.Contains(track.PreID, "ra.")) && Config.DlAlbumcoverForPlaylist {
			track.CoverPath, err = writeCover(track.SaveDir, track.ID, track.Resp.Attributes.Artwork.URL)
			if err != nil {
				fmt.Println("Failed to write cover.")
			}
		}
		tags = append(tags, fmt.Sprintf("cover=%s", track.CoverPath))
	}
	tagsString := strings.Join(tags, ":")
	cmd := exec.Command("MP4Box", "-itags", tagsString, trackPath)
    if err := cmd.Run(); err != nil {
        fmt.Printf("Embed failed: %v\n", err)
        counter.Error++
        return
    }
    if onSub != nil { onSub(100, "") }
	if (strings.Contains(track.PreID, "pl.") || strings.Contains(track.PreID, "ra.")) && Config.DlAlbumcoverForPlaylist {
		if err := os.Remove(track.CoverPath); err != nil {
			fmt.Printf("Error deleting file: %s\n", track.CoverPath)
			counter.Error++
			return
		}
	}
	track.SavePath = trackPath
	err = writeMP4Tags(track, lrc)
	if err != nil {
		fmt.Println("\u26A0 Failed to write tags in media:", err)
		counter.Unavailable++
		return
	}
	counter.Success++
	okDict[track.PreID] = append(okDict[track.PreID], track.TaskNum)
}

func ripStation(albumId string, token string, storefront string, mediaUserToken string, onSub func(int, string)) error {
	station := task.NewStation(storefront, albumId)
	err := station.GetResp(mediaUserToken, token, Config.Language)
	if err != nil {
		return err
	}
	fmt.Println(" -", station.Type)
	meta := station.Resp

	var Codec string
	if dl_atmos {
		Codec = "ATMOS"
	} else if dl_aac {
		Codec = "AAC"
	} else {
		Codec = "ALAC"
	}
	station.Codec = Codec
	var singerFoldername string
	if Config.ArtistFolderFormat != "" {
		singerFoldername = strings.NewReplacer(
			"{ArtistName}", "Apple Music Station",
			"{ArtistId}", "",
			"{UrlArtistName}", "Apple Music Station",
		).Replace(Config.ArtistFolderFormat)
		if strings.HasSuffix(singerFoldername, ".") {
			singerFoldername = strings.ReplaceAll(singerFoldername, ".", "")
		}
		singerFoldername = strings.TrimSpace(singerFoldername)
		fmt.Println(singerFoldername)
	}
	singerFolder := filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	if dl_atmos {
		singerFolder = filepath.Join(Config.AtmosSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	if dl_aac {
		singerFolder = filepath.Join(Config.AacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	os.MkdirAll(singerFolder, os.ModePerm)
	station.SaveDir = singerFolder

	playlistFolder := strings.NewReplacer(
		"{ArtistName}", "Apple Music Station",
		"{PlaylistName}", LimitString(station.Name),
		"{PlaylistId}", station.ID,
		"{Quality}", "",
		"{Codec}", Codec,
		"{Tag}", "",
	).Replace(Config.PlaylistFolderFormat)
	if strings.HasSuffix(playlistFolder, ".") {
		playlistFolder = strings.ReplaceAll(playlistFolder, ".", "")
	}
	playlistFolder = strings.TrimSpace(playlistFolder)
	playlistFolderPath := filepath.Join(singerFolder, forbiddenNames.ReplaceAllString(playlistFolder, "_"))
	os.MkdirAll(playlistFolderPath, os.ModePerm)
	station.SaveName = playlistFolder
	fmt.Println(playlistFolder)

	covPath, err := writeCover(playlistFolderPath, "cover", meta.Data[0].Attributes.Artwork.URL)
	if err != nil {
		fmt.Println("Failed to write cover.")
	}
	station.CoverPath = covPath

	if Config.SaveAnimatedArtwork && meta.Data[0].Attributes.EditorialVideo.MotionSquare.Video != "" {
		fmt.Println("Found Animation Artwork.")

		motionvideoUrlSquare, err := extractVideo(meta.Data[0].Attributes.EditorialVideo.MotionSquare.Video)
		if err != nil {
			fmt.Println("no motion video square.\n", err)
		} else {
			exists, err := fileExists(filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
			if err != nil {
				fmt.Println("Failed to check if animated artwork square exists.")
			}
			if exists {
				fmt.Println("Animated artwork square already exists locally.")
			} else {
				fmt.Println("Animation Artwork Square Downloading...")
				cmd := exec.Command("ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlSquare, "-c", "copy", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork square dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Square Downloaded")
				}
			}
		}

		if Config.EmbyAnimatedArtwork {
			cmd3 := exec.Command("ffmpeg", "-i", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"), "-vf", "scale=440:-1", "-r", "24", "-f", "gif", filepath.Join(playlistFolderPath, "folder.jpg"))
			if err := cmd3.Run(); err != nil {
				fmt.Printf("animated artwork square to gif err: %v\n", err)
			}
		}
	}
	if station.Type == "stream" {
		counter.Total++
		if isInArray(okDict[station.ID], 1) {
			counter.Success++
			return nil
		}
		songName := strings.NewReplacer(
			"{SongId}", station.ID,
			"{SongNumer}", "01",
			"{SongName}", LimitString(station.Name),
			"{DiscNumber}", "1",
			"{TrackNumber}", "1",
			"{Quality}", "256Kbps",
			"{Tag}", "",
			"{Codec}", "AAC",
		).Replace(Config.SongFileFormat)
		fmt.Println(songName)
		trackPath := filepath.Join(playlistFolderPath, fmt.Sprintf("%s.m4a", forbiddenNames.ReplaceAllString(songName, "_")))
		exists, _ := fileExists(trackPath)
		if exists {
			counter.Success++
			okDict[station.ID] = append(okDict[station.ID], 1)

			fmt.Println("Radio already exists locally.")
			return nil
		}
		assetsUrl, err := ampapi.GetStationAssetsUrl(station.ID, mediaUserToken, token)
		if err != nil {
			fmt.Println("Failed to get station assets url.", err)
			counter.Error++
			return err
		}
		trackM3U8 := strings.ReplaceAll(assetsUrl, "index.m3u8", "256/prog_index.m3u8")
		keyAndUrls, _ := runv3.Run(station.ID, trackM3U8, token, mediaUserToken, true)
		err = runv3.ExtMvData(keyAndUrls, trackPath)
		if err != nil {
			fmt.Println("Failed to download station stream.", err)
			counter.Error++
			return err
		}
		tags := []string{
			"tool=",
			"disk=1/1",
			"track=1",
			"tracknum=1/1",
			fmt.Sprintf("artist=%s", "Apple Music Station"),
			fmt.Sprintf("performer=%s", "Apple Music Station"),
			fmt.Sprintf("album_artist=%s", "Apple Music Station"),
			fmt.Sprintf("album=%s", station.Name),
			fmt.Sprintf("title=%s", station.Name),
		}
		if Config.EmbedCover {
			tags = append(tags, fmt.Sprintf("cover=%s", station.CoverPath))
		}
		tagsString := strings.Join(tags, ":")
		cmd := exec.Command("MP4Box", "-itags", tagsString, trackPath)
		if err := cmd.Run(); err != nil {
			fmt.Printf("Embed failed: %v\n", err)
		}
		counter.Success++
		okDict[station.ID] = append(okDict[station.ID], 1)
		return nil
	}

    for i := range station.Tracks {
		station.Tracks[i].CoverPath = covPath
		station.Tracks[i].SaveDir = playlistFolderPath
		station.Tracks[i].Codec = Codec
	}

	trackTotal := len(station.Tracks)
	arr := make([]int, trackTotal)
	for i := 0; i < trackTotal; i++ {
		arr[i] = i + 1
	}
	var selected []int

	if true {
		selected = arr
	}
	for i := range station.Tracks {
		i++
		if isInArray(selected, i) {
            if onSub != nil { onSub(0, "") }
            ripTrack(&station.Tracks[i-1], token, mediaUserToken, onSub)
            if onSub != nil { onSub(100, "") }
		}
	}
	return nil
}

func ripAlbum(
	albumId string,
	token string,
	storefront string,
	mediaUserToken string,
	urlArg_i string,
	selectedFromAPI []int,
	onProgress func(done, total int, msg string),
	onSub func(percent int, msg string),
) error {
	album := task.NewAlbum(storefront, albumId)
	if err := album.GetResp(token, Config.Language); err != nil {
		fmt.Println("Failed to get album response.")
		return err
	}
	meta := album.Resp

	// debug 模式下仅探测音质信息
	if debug_mode {
		fmt.Println(meta.Data[0].Attributes.ArtistName)
		fmt.Println(meta.Data[0].Attributes.Name)

		for trackNum, track := range meta.Data[0].Relationships.Tracks.Data {
			trackNum++
			fmt.Printf("\nTrack %d of %d:\n", trackNum, len(meta.Data[0].Relationships.Tracks.Data))
			fmt.Printf("%02d. %s\n", trackNum, track.Attributes.Name)

			manifest, err := ampapi.GetSongResp(storefront, track.ID, album.Language, token)
			if err != nil {
				fmt.Printf("Failed to get manifest for track %d: %v\n", trackNum, err)
				continue
			}

			var m3u8Url string
			if manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls != "" {
				m3u8Url = manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls
			}
			needCheck := false
			if Config.GetM3u8Mode == "all" ||
				(Config.GetM3u8Mode == "hires" && contains(track.Attributes.AudioTraits, "hi-res-lossless")) {
				needCheck = true
			}
			if needCheck {
				if full, err := checkM3u8(track.ID, "song"); err == nil && strings.HasSuffix(full, ".m3u8") {
					m3u8Url = full
				} else {
					fmt.Println("Failed to get best quality m3u8 from device m3u8 port, will use m3u8 from Web API")
				}
			}

			if _, _, err := extractMedia(m3u8Url, true); err != nil {
				fmt.Printf("Failed to extract quality info for track %d: %v\n", trackNum, err)
				continue
			}
		}
		return nil
	}

	// 选择最终编码标签
	var Codec string
	switch {
	case dl_atmos:
		Codec = "ATMOS"
	case dl_aac:
		Codec = "AAC"
	default:
		Codec = "ALAC"
	}
	album.Codec = Codec

	// 生成歌手文件夹
	var singerFoldername string
	if Config.ArtistFolderFormat != "" {
		if len(meta.Data[0].Relationships.Artists.Data) > 0 {
			singerFoldername = strings.NewReplacer(
				"{UrlArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
				"{ArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
				"{ArtistId}", meta.Data[0].Relationships.Artists.Data[0].ID,
			).Replace(Config.ArtistFolderFormat)
		} else {
			singerFoldername = strings.NewReplacer(
				"{UrlArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
				"{ArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
				"{ArtistId}", "",
			).Replace(Config.ArtistFolderFormat)
		}
		if strings.HasSuffix(singerFoldername, ".") {
			singerFoldername = strings.ReplaceAll(singerFoldername, ".", "")
		}
		singerFoldername = strings.TrimSpace(singerFoldername)
		fmt.Println(singerFoldername)
	}

	singerFolder := filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	if dl_atmos {
		singerFolder = filepath.Join(Config.AtmosSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	if dl_aac {
		singerFolder = filepath.Join(Config.AacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	_ = os.MkdirAll(singerFolder, os.ModePerm)
	album.SaveDir = singerFolder

	// 质量字符串（用于命名）
	var Quality string
	if strings.Contains(Config.AlbumFolderFormat, "Quality") {
		if dl_atmos {
			Quality = fmt.Sprintf("%dKbps", Config.AtmosMax-2000)
		} else if dl_aac && Config.AacType == "aac-lc" {
			Quality = "256Kbps"
		} else {
			manifest1, err := ampapi.GetSongResp(storefront, meta.Data[0].Relationships.Tracks.Data[0].ID, album.Language, token)
			if err == nil {
				if manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls == "" {
					Codec = "AAC"
					Quality = "256Kbps"
				} else {
					needCheck := false
					if Config.GetM3u8Mode == "all" ||
						(Config.GetM3u8Mode == "hires" && contains(meta.Data[0].Relationships.Tracks.Data[0].Attributes.AudioTraits, "hi-res-lossless")) {
						needCheck = true
					}
					if needCheck {
						if full, _ := checkM3u8(meta.Data[0].Relationships.Tracks.Data[0].ID, "album"); strings.HasSuffix(full, ".m3u8") {
							manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls = full
						}
					}
					if _, q, err := extractMedia(manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls, true); err == nil {
						Quality = q
					}
				}
			} else {
				fmt.Println("Failed to get manifest.\n", err)
			}
		}
	}

	// 标签
	var parts []string
	if meta.Data[0].Attributes.IsAppleDigitalMaster || meta.Data[0].Attributes.IsMasteredForItunes {
		if Config.AppleMasterChoice != "" {
			parts = append(parts, Config.AppleMasterChoice)
		}
	}
	switch meta.Data[0].Attributes.ContentRating {
	case "explicit":
		if Config.ExplicitChoice != "" {
			parts = append(parts, Config.ExplicitChoice)
		}
	case "clean":
		if Config.CleanChoice != "" {
			parts = append(parts, Config.CleanChoice)
		}
	}
	Tag_string := strings.Join(parts, " ")

	// 专辑目录
	albumFolderName := strings.NewReplacer(
		"{ReleaseDate}", meta.Data[0].Attributes.ReleaseDate,
		"{ReleaseYear}", meta.Data[0].Attributes.ReleaseDate[:4],
		"{ArtistName}", LimitString(meta.Data[0].Attributes.ArtistName),
		"{AlbumName}", LimitString(meta.Data[0].Attributes.Name),
		"{UPC}", meta.Data[0].Attributes.Upc,
		"{RecordLabel}", meta.Data[0].Attributes.RecordLabel,
		"{Copyright}", meta.Data[0].Attributes.Copyright,
		"{AlbumId}", albumId,
		"{Quality}", Quality,
		"{Codec}", Codec,
		"{Tag}", Tag_string,
	).Replace(Config.AlbumFolderFormat)
	if strings.HasSuffix(albumFolderName, ".") {
		albumFolderName = strings.ReplaceAll(albumFolderName, ".", "")
	}
	albumFolderName = strings.TrimSpace(albumFolderName)

	albumFolderPath := filepath.Join(singerFolder, forbiddenNames.ReplaceAllString(albumFolderName, "_"))
	_ = os.MkdirAll(albumFolderPath, os.ModePerm)
	album.SaveName = albumFolderName
	fmt.Println(albumFolderName)

	// 封面与动画封面
	if Config.SaveArtistCover && len(meta.Data[0].Relationships.Artists.Data) > 0 {
		if _, err := writeCover(singerFolder, "folder", meta.Data[0].Relationships.Artists.Data[0].Attributes.Artwork.Url); err != nil {
			fmt.Println("Failed to write artist cover.")
		}
	}
	covPath, _ := writeCover(albumFolderPath, "cover", meta.Data[0].Attributes.Artwork.URL)

	if Config.SaveAnimatedArtwork && meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video != "" {
		fmt.Println("Found Animation Artwork.")
		if url, err := extractVideo(meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video); err == nil {
			if ok, _ := fileExists(filepath.Join(albumFolderPath, "square_animated_artwork.mp4")); !ok {
				fmt.Println("Animation Artwork Square Downloading...")
				cmd := exec.Command("ffmpeg", "-loglevel", "quiet", "-y", "-i", url, "-c", "copy", filepath.Join(albumFolderPath, "square_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork square dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Square Downloaded")
				}
			}
		}
		if Config.EmbyAnimatedArtwork {
			cmd3 := exec.Command("ffmpeg", "-i", filepath.Join(albumFolderPath, "square_animated_artwork.mp4"), "-vf", "scale=440:-1", "-r", "24", "-f", "gif", filepath.Join(albumFolderPath, "folder.jpg"))
			_ = cmd3.Run()
		}
		if url, err := extractVideo(meta.Data[0].Attributes.EditorialVideo.MotionDetailTall.Video); err == nil {
			if ok, _ := fileExists(filepath.Join(albumFolderPath, "tall_animated_artwork.mp4")); !ok {
				fmt.Println("Animation Artwork Tall Downloading...")
				cmd := exec.Command("ffmpeg", "-loglevel", "quiet", "-y", "-i", url, "-c", "copy", filepath.Join(albumFolderPath, "tall_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork tall dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Tall Downloaded")
				}
			}
		}
	}

	// 初始化每首曲目的保存参数
	for i := range album.Tracks {
		album.Tracks[i].CoverPath = covPath
		album.Tracks[i].SaveDir = albumFolderPath
		album.Tracks[i].Codec = Codec
	}

	// === 从这里开始：进度与选择 ===
	totalAll := len(meta.Data[0].Relationships.Tracks.Data)
	if totalAll <= 0 {
		if onProgress != nil {
			onProgress(0, 1, "no tracks")
		}
		return nil
	}

	// 单曲模式：仅下载 ?i= 指定的曲目（带进度）
	if dl_song && urlArg_i != "" {
		if onProgress != nil {
			onProgress(0, 1, "start single track")
		}
        for i := range album.Tracks {
            if urlArg_i == album.Tracks[i].ID {
                if onSub != nil { onSub(0, album.Tracks[i].Resp.Attributes.Name) }
                ripTrack(&album.Tracks[i], token, mediaUserToken, onSub)
                if onProgress != nil {
                    onProgress(1, 1, fmt.Sprintf("done: %s", album.Tracks[i].Resp.Attributes.Name))
                }
                if onSub != nil { onSub(100, "") }
                return nil
            }
        }
		// 没找到也结束
		if onProgress != nil {
			onProgress(1, 1, "single track not found")
		}
		return nil
	}

	// 选曲：如果未指定，则默认全选
	selected := selectedFromAPI
	if len(selected) == 0 {
		selected = make([]int, totalAll)
		for i := range selected {
			selected[i] = i + 1
		}
	}

	done := 0
	total := len(selected)
	if onProgress != nil {
		onProgress(0, total, "start album")
	}
    for i := range album.Tracks {
        idx := i + 1
        if isInArray(selected, idx) {
            if onSub != nil { onSub(0, "") }
            ripTrack(&album.Tracks[i], token, mediaUserToken, onSub)
            done++
            if onProgress != nil {
                onProgress(done, total, fmt.Sprintf("done track %d/%d", done, total))
            }
            if onSub != nil { onSub(100, "") }
        }
    }
	return nil
}

func ripPlaylist(playlistId string, token string, storefront string, mediaUserToken string, selectedFromAPI []int, onProgress func(done, total int, msg string), onSub func(percent int, msg string)) error {
	playlist := task.NewPlaylist(storefront, playlistId)
	err := playlist.GetResp(token, Config.Language)
	if err != nil {
		fmt.Println("Failed to get playlist response.")
		return err
	}
	meta := playlist.Resp

	// 调试模式：只展示可用音频/码率信息后返回
	if debug_mode {
		fmt.Println(meta.Data[0].Attributes.ArtistName)
		fmt.Println(meta.Data[0].Attributes.Name)

		for trackNum, track := range meta.Data[0].Relationships.Tracks.Data {
			trackNum++
			fmt.Printf("\nTrack %d of %d:\n", trackNum, len(meta.Data[0].Relationships.Tracks.Data))
			fmt.Printf("%02d. %s\n", trackNum, track.Attributes.Name)

			manifest, err := ampapi.GetSongResp(storefront, track.ID, playlist.Language, token)
			if err != nil {
				fmt.Printf("Failed to get manifest for track %d: %v\n", trackNum, err)
				continue
			}

			var m3u8Url string
			if manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls != "" {
				m3u8Url = manifest.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls
			}
			needCheck := false
			if Config.GetM3u8Mode == "all" {
				needCheck = true
			} else if Config.GetM3u8Mode == "hires" && contains(track.Attributes.AudioTraits, "hi-res-lossless") {
				needCheck = true
			}
			if needCheck {
				fullM3u8Url, err := checkM3u8(track.ID, "song")
				if err == nil && strings.HasSuffix(fullM3u8Url, ".m3u8") {
					m3u8Url = fullM3u8Url
				} else {
					fmt.Println("Failed to get best quality m3u8 from device m3u8 port, will use m3u8 from Web API")
				}
			}

			_, _, err = extractMedia(m3u8Url, true)
			if err != nil {
				fmt.Printf("Failed to extract quality info for track %d: %v\n", trackNum, err)
				continue
			}
		}
		return nil
	}

	// 编码类型
	var Codec string
	if dl_atmos {
		Codec = "ATMOS"
	} else if dl_aac {
		Codec = "AAC"
	} else {
		Codec = "ALAC"
	}
	playlist.Codec = Codec

	// “Apple Music” 目录
	var singerFoldername string
	if Config.ArtistFolderFormat != "" {
		singerFoldername = strings.NewReplacer(
			"{ArtistName}", "Apple Music",
			"{ArtistId}", "",
			"{UrlArtistName}", "Apple Music",
		).Replace(Config.ArtistFolderFormat)
		if strings.HasSuffix(singerFoldername, ".") {
			singerFoldername = strings.ReplaceAll(singerFoldername, ".", "")
		}
		singerFoldername = strings.TrimSpace(singerFoldername)
		fmt.Println(singerFoldername)
	}
	singerFolder := filepath.Join(Config.AlacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	if dl_atmos {
		singerFolder = filepath.Join(Config.AtmosSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	if dl_aac {
		singerFolder = filepath.Join(Config.AacSaveFolder, forbiddenNames.ReplaceAllString(singerFoldername, "_"))
	}
	_ = os.MkdirAll(singerFolder, os.ModePerm)
	playlist.SaveDir = singerFolder

	// 质量标签（仅用于命名）
	var Quality string
	if strings.Contains(Config.AlbumFolderFormat, "Quality") {
		if dl_atmos {
			Quality = fmt.Sprintf("%dKbps", Config.AtmosMax-2000)
		} else if dl_aac && Config.AacType == "aac-lc" {
			Quality = "256Kbps"
		} else {
			manifest1, err := ampapi.GetSongResp(storefront, meta.Data[0].Relationships.Tracks.Data[0].ID, playlist.Language, token)
			if err != nil {
				fmt.Println("Failed to get manifest.\n", err)
			} else {
				if manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls == "" {
					Codec = "AAC"
					Quality = "256Kbps"
				} else {
					needCheck := false
					if Config.GetM3u8Mode == "all" {
						needCheck = true
					} else if Config.GetM3u8Mode == "hires" && contains(meta.Data[0].Relationships.Tracks.Data[0].Attributes.AudioTraits, "hi-res-lossless") {
						needCheck = true
					}
					if needCheck {
						if enhanced, _ := checkM3u8(meta.Data[0].Relationships.Tracks.Data[0].ID, "album"); strings.HasSuffix(enhanced, ".m3u8") {
							manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls = enhanced
						}
					}
					if _, q, err := extractMedia(manifest1.Data[0].Attributes.ExtendedAssetUrls.EnhancedHls, true); err == nil {
						Quality = q
					}
				}
			}
		}
	}

	// 标记/命名
	var tagsForName []string
	if meta.Data[0].Attributes.IsAppleDigitalMaster || meta.Data[0].Attributes.IsMasteredForItunes {
		if Config.AppleMasterChoice != "" {
			tagsForName = append(tagsForName, Config.AppleMasterChoice)
		}
	}
	if meta.Data[0].Attributes.ContentRating == "explicit" && Config.ExplicitChoice != "" {
		tagsForName = append(tagsForName, Config.ExplicitChoice)
	}
	if meta.Data[0].Attributes.ContentRating == "clean" && Config.CleanChoice != "" {
		tagsForName = append(tagsForName, Config.CleanChoice)
	}
	Tag_string := strings.Join(tagsForName, " ")

	// 歌单目录
	playlistFolder := strings.NewReplacer(
		"{ArtistName}", "Apple Music",
		"{PlaylistName}", LimitString(meta.Data[0].Attributes.Name),
		"{PlaylistId}", playlistId,
		"{Quality}", Quality,
		"{Codec}", Codec,
		"{Tag}", Tag_string,
	).Replace(Config.PlaylistFolderFormat)
	if strings.HasSuffix(playlistFolder, ".") {
		playlistFolder = strings.ReplaceAll(playlistFolder, ".", "")
	}
	playlistFolder = strings.TrimSpace(playlistFolder)
	playlistFolderPath := filepath.Join(singerFolder, forbiddenNames.ReplaceAllString(playlistFolder, "_"))
	_ = os.MkdirAll(playlistFolderPath, os.ModePerm)
	playlist.SaveName = playlistFolder
	fmt.Println(playlistFolder)

	// 封面
	covPath, err := writeCover(playlistFolderPath, "cover", meta.Data[0].Attributes.Artwork.URL)
	if err != nil {
		fmt.Println("Failed to write cover.")
	}

	// 绑定轨道路径
	for i := range playlist.Tracks {
		playlist.Tracks[i].CoverPath = covPath
		playlist.Tracks[i].SaveDir = playlistFolderPath
		playlist.Tracks[i].Codec = Codec
	}

	// 动画封面（与原逻辑一致）
	if Config.SaveAnimatedArtwork && meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video != "" {
		fmt.Println("Found Animation Artwork.")

		if motionvideoUrlSquare, err := extractVideo(meta.Data[0].Attributes.EditorialVideo.MotionDetailSquare.Video); err == nil {
			exists, _ := fileExists(filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
			if !exists {
				fmt.Println("Animation Artwork Square Downloading...")
				cmd := exec.Command("ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlSquare, "-c", "copy", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork square dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Square Downloaded")
				}
			}
		} else {
			fmt.Println("no motion video square.\n", err)
		}

		if Config.EmbyAnimatedArtwork {
			cmd3 := exec.Command("ffmpeg", "-i", filepath.Join(playlistFolderPath, "square_animated_artwork.mp4"), "-vf", "scale=440:-1", "-r", "24", "-f", "gif", filepath.Join(playlistFolderPath, "folder.jpg"))
			if err := cmd3.Run(); err != nil {
				fmt.Printf("animated artwork square to gif err: %v\n", err)
			}
		}

		if motionvideoUrlTall, err := extractVideo(meta.Data[0].Attributes.EditorialVideo.MotionDetailTall.Video); err == nil {
			exists, _ := fileExists(filepath.Join(playlistFolderPath, "tall_animated_artwork.mp4"))
			if !exists {
				fmt.Println("Animation Artwork Tall Downloading...")
				cmd := exec.Command("ffmpeg", "-loglevel", "quiet", "-y", "-i", motionvideoUrlTall, "-c", "copy", filepath.Join(playlistFolderPath, "tall_animated_artwork.mp4"))
				if err := cmd.Run(); err != nil {
					fmt.Printf("animated artwork tall dl err: %v\n", err)
				} else {
					fmt.Println("Animation Artwork Tall Downloaded")
				}
			}
		} else {
			fmt.Println("no motion video tall.\n", err)
		}
	}

	// ====== 进度 & 选曲 ======
	trackTotalAll := len(meta.Data[0].Relationships.Tracks.Data)

	// 全量曲目数组（修复 undefined: arr）
	fullArr := make([]int, trackTotalAll)
	for i := 0; i < trackTotalAll; i++ {
		fullArr[i] = i + 1
	}

	var selected []int
	if len(selectedFromAPI) > 0 {
		selected = selectedFromAPI
	} else {
		selected = fullArr
	}

	// 以选择的曲目数作为总进度分母
	trackTotal := len(selected)
	if onProgress != nil {
		onProgress(0, trackTotal, "start playlist")
	}

    done := 0
    for i := range playlist.Tracks {
        idx := i + 1
        if isInArray(selected, idx) {
            if onSub != nil { onSub(0, playlist.Tracks[i].Resp.Attributes.Name) }
            ripTrack(&playlist.Tracks[i], token, mediaUserToken, onSub)
            done++
            if onProgress != nil {
                onProgress(done, trackTotal, fmt.Sprintf("done track %d/%d", done, trackTotal))
            }
            if onSub != nil { onSub(100, "") }
        }
    }
	return nil
}

func writeMP4Tags(track *task.Track, lrc string) error {
	t := &mp4tag.MP4Tags{
		Title:      track.Resp.Attributes.Name,
		TitleSort:  track.Resp.Attributes.Name,
		Artist:     track.Resp.Attributes.ArtistName,
		ArtistSort: track.Resp.Attributes.ArtistName,
		Custom: map[string]string{
			"PERFORMER":   track.Resp.Attributes.ArtistName,
			"RELEASETIME": track.Resp.Attributes.ReleaseDate,
			"ISRC":        track.Resp.Attributes.Isrc,
			"LABEL":       "",
			"UPC":         "",
		},
		Composer:     track.Resp.Attributes.ComposerName,
		ComposerSort: track.Resp.Attributes.ComposerName,
		CustomGenre:  track.Resp.Attributes.GenreNames[0],
		Lyrics:       lrc,
		TrackNumber:  int16(track.Resp.Attributes.TrackNumber),
		DiscNumber:   int16(track.Resp.Attributes.DiscNumber),
		Album:        track.Resp.Attributes.AlbumName,
		AlbumSort:    track.Resp.Attributes.AlbumName,
	}

	if track.PreType == "albums" {
		albumID, err := strconv.ParseUint(track.PreID, 10, 32)
		if err != nil {
			return err
		}
		t.ItunesAlbumID = int32(albumID)
	}

	if len(track.Resp.Relationships.Artists.Data) > 0 {
		artistID, err := strconv.ParseUint(track.Resp.Relationships.Artists.Data[0].ID, 10, 32)
		if err != nil {
			return err
		}
		t.ItunesArtistID = int32(artistID)
	}

	if (track.PreType == "playlists" || track.PreType == "stations") && !Config.UseSongInfoForPlaylist {
		t.DiscNumber = 1
		t.DiscTotal = 1
		t.TrackNumber = int16(track.TaskNum)
		t.TrackTotal = int16(track.TaskTotal)
		t.Album = track.PlaylistData.Attributes.Name
		t.AlbumSort = track.PlaylistData.Attributes.Name
		t.AlbumArtist = track.PlaylistData.Attributes.ArtistName
		t.AlbumArtistSort = track.PlaylistData.Attributes.ArtistName
	} else if (track.PreType == "playlists" || track.PreType == "stations") && Config.UseSongInfoForPlaylist {
		t.DiscTotal = int16(track.DiscTotal)
		t.TrackTotal = int16(track.AlbumData.Attributes.TrackCount)
		t.AlbumArtist = track.AlbumData.Attributes.ArtistName
		t.AlbumArtistSort = track.AlbumData.Attributes.ArtistName
		t.Custom["UPC"] = track.AlbumData.Attributes.Upc
		t.Custom["LABEL"] = track.AlbumData.Attributes.RecordLabel
		t.Date = track.AlbumData.Attributes.ReleaseDate
		t.Copyright = track.AlbumData.Attributes.Copyright
		t.Publisher = track.AlbumData.Attributes.RecordLabel
	} else {
		t.DiscTotal = int16(track.DiscTotal)
		t.TrackTotal = int16(track.AlbumData.Attributes.TrackCount)
		t.AlbumArtist = track.AlbumData.Attributes.ArtistName
		t.AlbumArtistSort = track.AlbumData.Attributes.ArtistName
		t.Custom["UPC"] = track.AlbumData.Attributes.Upc
		t.Date = track.AlbumData.Attributes.ReleaseDate
		t.Copyright = track.AlbumData.Attributes.Copyright
		t.Publisher = track.AlbumData.Attributes.RecordLabel
	}

	if track.Resp.Attributes.ContentRating == "explicit" {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryExplicit
	} else if track.Resp.Attributes.ContentRating == "clean" {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryClean
	} else {
		t.ItunesAdvisory = mp4tag.ItunesAdvisoryNone
	}

	mp4, err := mp4tag.Open(track.SavePath)
	if err != nil {
		return err
	}
	defer mp4.Close()
	err = mp4.Write(t, []string{})
	if err != nil {
		return err
	}
	return nil
}

func main() {
	err := loadConfig()
	if err != nil {
		log.Fatalf("load Config failed: %v", err)
	}

	token, err := ampapi.GetToken()
	if err != nil {
		if Config.AuthorizationToken != "" && Config.AuthorizationToken != "your-authorization-token" {
			token = strings.Replace(Config.AuthorizationToken, "Bearer ", "", -1)
		} else {
			log.Fatalf("Failed to get token.")
		}
	}

	// 初始化任务管理器与下载执行器
	mgr := NewTaskManager(2, 128) // 2 个 worker，队列 128

    mgr.BindRunner(func(t *Task) error {
		dl_atmos, dl_aac, dl_song = false, false, false
		switch t.Quality {
		case "atmos":
			dl_atmos = true
		case "aac":
			dl_aac = true
		}

		if t.SongOnly {
			dl_song = true
		}

        appendLog := func(msg string) { mgr.AppendLog(t.ID, msg) }
        setSub := func(p int, msg string) { mgr.SetSubProgress(t.ID, p, msg) }

		// 取消检查辅助
		canceled := func() bool {
			if t.Canceled {
				appendLog("canceled")
				return true
			}
			return false
		}

		setProgress := func(done, total int, msg string) {
			mgr.mu.Lock()
			if total <= 0 {
				total = 1
			}
			t.TotalUnits = total
			t.DoneUnits = done
			t.Progress = int(float64(done) / float64(total) * 100.0)
			if t.Progress > 100 {
				t.Progress = 100
			}
			if msg != "" {
				t.Logs = append(t.Logs, msg)
				t.Message = msg
                if len(t.Logs) > 1000 {
                    t.Logs = t.Logs[len(t.Logs)-1000:]
                }
			}
			mgr.mu.Unlock()
		}

		urlRaw := t.URL
		if strings.Contains(urlRaw, "/song/") {
			u, err := getUrlSong(urlRaw, t.Token)
			if err != nil {
				appendLog("Failed to get Song info: " + err.Error())
				return err
			}
			urlRaw = u
			dl_song = true
		}

		var urlArg_i string
		if p, err := url.Parse(urlRaw); err == nil {
			urlArg_i = p.Query().Get("i")
		}

        runOnce := func() error {
            switch {
            case strings.Contains(urlRaw, "/music-video/"):
                appendLog("Type: Music Video")
                setProgress(0, 4, "prepare mv")
                setSub(0, "")
                if len(Config.MediaUserToken) <= 50 {
                    appendLog("media-user-token not set, skip MV")
                    setProgress(4, 4, "mv skipped")
                    setSub(100, "")
                    return nil
                }
                _, mvID := checkUrlMv(urlRaw)
                saveDir := Config.AlacSaveFolder
                if err := mvDownloader(mvID, saveDir, t.Token, Config.Storefront, Config.MediaUserToken, nil); err != nil {
                    return err
                }
                setProgress(4, 4, "mv done")
                setSub(100, "")
                return nil

            case strings.Contains(urlRaw, "/album/"):
                appendLog("Type: Album")
                storefront, albumId := checkUrl(urlRaw)
                return ripAlbum(albumId, t.Token, storefront, Config.MediaUserToken, urlArg_i, t.Tracks, setProgress, setSub)

            case strings.Contains(urlRaw, "/playlist/"):
                appendLog("Type: Playlist")
                storefront, pid := checkUrlPlaylist(urlRaw)
                return ripPlaylist(pid, t.Token, storefront, Config.MediaUserToken, t.Tracks, setProgress, setSub)

            case strings.Contains(urlRaw, "/station/"):
                appendLog("Type: Station")
                storefront, sid := checkUrlStation(urlRaw)
                if len(Config.MediaUserToken) <= 50 {
                    appendLog("media-user-token not set, skip station")
                    return nil
                }
                setProgress(0, 3, "prepare station")
                if err := ripStation(sid, t.Token, storefront, Config.MediaUserToken, setSub); err != nil {
                    return err
                }
                setProgress(3, 3, "station done")
                setSub(100, "")
                return nil

			default:
				return fmt.Errorf("invalid url type")
			}
		}

		var err error
		for attempt := 0; attempt <= t.MaxRetries; attempt++ {
			if canceled() {
				return fmt.Errorf("canceled")
			}
			if attempt > 0 {
				appendLog(fmt.Sprintf("retry %d...", attempt))
				for i := 0; i < 3; i++ { // 可中断等待
					if canceled() { return fmt.Errorf("canceled") }
					time.Sleep(time.Second)
				}
			}
			err = runOnce()
			if err == nil {
				break
			}
			appendLog("error: " + err.Error())
		}
		return err
	})

	// 启动 HTTP API
	r := gin.Default()
	// Web UI 静态资源
    r.Static("/static", "./web")
    r.GET("/", func(c *gin.Context) { c.File("web/index.html") })
	registerRoutes(r, mgr, token)
	log.Println("HTTP server listening on :8080")
	if err := r.Run(":8080"); err != nil {
		log.Fatal(err)
	}
}

func mvDownloader(adamID string, saveDir string, token string, storefront string, mediaUserToken string, track *task.Track) error {
	MVInfo, err := ampapi.GetMusicVideoResp(storefront, adamID, Config.Language, token)
	if err != nil {
		fmt.Println("\u26A0 Failed to get MV manifest:", err)
		return nil
	}

	if strings.HasSuffix(saveDir, ".") {
		saveDir = strings.ReplaceAll(saveDir, ".", "")
	}
	saveDir = strings.TrimSpace(saveDir)

	vidPath := filepath.Join(saveDir, fmt.Sprintf("%s_vid.mp4", adamID))
	audPath := filepath.Join(saveDir, fmt.Sprintf("%s_aud.mp4", adamID))
	mvSaveName := fmt.Sprintf("%s (%s)", MVInfo.Data[0].Attributes.Name, adamID)
	if track != nil {
		mvSaveName = fmt.Sprintf("%02d. %s", track.TaskNum, MVInfo.Data[0].Attributes.Name)
	}

	mvOutPath := filepath.Join(saveDir, fmt.Sprintf("%s.mp4", forbiddenNames.ReplaceAllString(mvSaveName, "_")))

	fmt.Println(MVInfo.Data[0].Attributes.Name)

	exists, _ := fileExists(mvOutPath)
	if exists {
		fmt.Println("MV already exists locally.")
		return nil
	}

	mvm3u8url, _, _ := runv3.GetWebplayback(adamID, token, mediaUserToken, true)
	if mvm3u8url == "" {
		return errors.New("media-user-token may wrong or expired")
	}

	os.MkdirAll(saveDir, os.ModePerm)
	videom3u8url, _ := extractVideo(mvm3u8url)
	videokeyAndUrls, _ := runv3.Run(adamID, videom3u8url, token, mediaUserToken, true)
	_ = runv3.ExtMvData(videokeyAndUrls, vidPath)
	audiom3u8url, _ := extractMvAudio(mvm3u8url)
	audiokeyAndUrls, _ := runv3.Run(adamID, audiom3u8url, token, mediaUserToken, true)
	_ = runv3.ExtMvData(audiokeyAndUrls, audPath)

	tags := []string{
		"tool=",
		fmt.Sprintf("artist=%s", MVInfo.Data[0].Attributes.ArtistName),
		fmt.Sprintf("title=%s", MVInfo.Data[0].Attributes.Name),
		fmt.Sprintf("genre=%s", MVInfo.Data[0].Attributes.GenreNames[0]),
		fmt.Sprintf("created=%s", MVInfo.Data[0].Attributes.ReleaseDate),
		fmt.Sprintf("ISRC=%s", MVInfo.Data[0].Attributes.Isrc),
	}

	if MVInfo.Data[0].Attributes.ContentRating == "explicit" {
		tags = append(tags, "rating=1")
	} else if MVInfo.Data[0].Attributes.ContentRating == "clean" {
		tags = append(tags, "rating=2")
	} else {
		tags = append(tags, "rating=0")
	}

	if track != nil {
		if track.PreType == "playlists" && !Config.UseSongInfoForPlaylist {
			tags = append(tags, "disk=1/1")
			tags = append(tags, fmt.Sprintf("album=%s", track.PlaylistData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("track=%d", track.TaskNum))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.TaskNum, track.TaskTotal))
			tags = append(tags, fmt.Sprintf("album_artist=%s", track.PlaylistData.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("performer=%s", track.Resp.Attributes.ArtistName))
		} else if track.PreType == "playlists" && Config.UseSongInfoForPlaylist {
			tags = append(tags, fmt.Sprintf("album=%s", track.AlbumData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("disk=%d/%d", track.Resp.Attributes.DiscNumber, track.DiscTotal))
			tags = append(tags, fmt.Sprintf("track=%d", track.Resp.Attributes.TrackNumber))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.Resp.Attributes.TrackNumber, track.AlbumData.Attributes.TrackCount))
			tags = append(tags, fmt.Sprintf("album_artist=%s", track.AlbumData.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("performer=%s", track.Resp.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("copyright=%s", track.AlbumData.Attributes.Copyright))
			tags = append(tags, fmt.Sprintf("UPC=%s", track.AlbumData.Attributes.Upc))
		} else {
			tags = append(tags, fmt.Sprintf("album=%s", track.AlbumData.Attributes.Name))
			tags = append(tags, fmt.Sprintf("disk=%d/%d", track.Resp.Attributes.DiscNumber, track.DiscTotal))
			tags = append(tags, fmt.Sprintf("track=%d", track.Resp.Attributes.TrackNumber))
			tags = append(tags, fmt.Sprintf("tracknum=%d/%d", track.Resp.Attributes.TrackNumber, track.AlbumData.Attributes.TrackCount))
			tags = append(tags, fmt.Sprintf("album_artist=%s", track.AlbumData.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("performer=%s", track.Resp.Attributes.ArtistName))
			tags = append(tags, fmt.Sprintf("copyright=%s", track.AlbumData.Attributes.Copyright))
			tags = append(tags, fmt.Sprintf("UPC=%s", track.AlbumData.Attributes.Upc))
		}
	} else {
		tags = append(tags, fmt.Sprintf("album=%s", MVInfo.Data[0].Attributes.AlbumName))
		tags = append(tags, fmt.Sprintf("disk=%d", MVInfo.Data[0].Attributes.DiscNumber))
		tags = append(tags, fmt.Sprintf("track=%d", MVInfo.Data[0].Attributes.TrackNumber))
		tags = append(tags, fmt.Sprintf("tracknum=%d", MVInfo.Data[0].Attributes.TrackNumber))
		tags = append(tags, fmt.Sprintf("performer=%s", MVInfo.Data[0].Attributes.ArtistName))
	}

	var covPath string
	if true {
		thumbURL := MVInfo.Data[0].Attributes.Artwork.URL
		baseThumbName := forbiddenNames.ReplaceAllString(mvSaveName, "_") + "_thumbnail"
		covPath, err = writeCover(saveDir, baseThumbName, thumbURL)
		if err != nil {
			fmt.Println("Failed to save MV thumbnail:", err)
		} else {
			tags = append(tags, fmt.Sprintf("cover=%s", covPath))
		}
	}

	tagsString := strings.Join(tags, ":")
	muxCmd := exec.Command("MP4Box", "-itags", tagsString, "-quiet", "-add", vidPath, "-add", audPath, "-keep-utc", "-new", mvOutPath)
	fmt.Printf("MV Remuxing...")
	if err := muxCmd.Run(); err != nil {
		fmt.Printf("MV mux failed: %v\n", err)
		return err
	}
	fmt.Printf("\rMV Remuxed.   \n")
	defer os.Remove(vidPath)
	defer os.Remove(audPath)
	defer os.Remove(covPath)

	return nil
}

func extractMvAudio(c string) (string, error) {
	MediaUrl, err := url.Parse(c)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(c)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}

	audioString := string(body)
	from, listType, err := m3u8.DecodeFrom(strings.NewReader(audioString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", errors.New("m3u8 not of media type")
	}

	audio := from.(*m3u8.MasterPlaylist)

	var audioPriority = []string{"audio-atmos", "audio-ac3", "audio-stereo-256"}
	if Config.MVAudioType == "ac3" {
		audioPriority = []string{"audio-ac3", "audio-stereo-256"}
	} else if Config.MVAudioType == "aac" {
		audioPriority = []string{"audio-stereo-256"}
	}

	re := regexp.MustCompile(`_gr(\d+)_`)

	type AudioStream struct {
		URL     string
		Rank    int
		GroupID string
	}
	var audioStreams []AudioStream

	for _, variant := range audio.Variants {
		for _, audiov := range variant.Alternatives {
			if audiov.URI != "" {
				for _, priority := range audioPriority {
					if audiov.GroupId == priority {
						matches := re.FindStringSubmatch(audiov.URI)
						if len(matches) == 2 {
							var rank int
							fmt.Sscanf(matches[1], "%d", &rank)
							streamUrl, _ := MediaUrl.Parse(audiov.URI)
							audioStreams = append(audioStreams, AudioStream{
								URL:     streamUrl.String(),
								Rank:    rank,
								GroupID: audiov.GroupId,
							})
						}
					}
				}
			}
		}
	}

	if len(audioStreams) == 0 {
		return "", errors.New("no suitable audio stream found")
	}

	sort.Slice(audioStreams, func(i, j int) bool {
		return audioStreams[i].Rank > audioStreams[j].Rank
	})
	fmt.Println("Audio: " + audioStreams[0].GroupID)
	return audioStreams[0].URL, nil
}

func checkM3u8(b string, f string) (string, error) {
	var EnhancedHls string
	if Config.GetM3u8FromDevice {
		adamID := b
		conn, err := net.Dial("tcp", Config.GetM3u8Port)
		if err != nil {
			fmt.Println("Error connecting to device:", err)
			return "none", err
		}
		defer conn.Close()
		if f == "song" {
			fmt.Println("Connected to device")
		}

		adamIDBuffer := []byte(adamID)
		lengthBuffer := []byte{byte(len(adamIDBuffer))}

		_, err = conn.Write(lengthBuffer)
		if err != nil {
			fmt.Println("Error writing length to device:", err)
			return "none", err
		}

		_, err = conn.Write(adamIDBuffer)
		if err != nil {
			fmt.Println("Error writing adamID to device:", err)
			return "none", err
		}

		response, err := bufio.NewReader(conn).ReadBytes('\n')
		if err != nil {
			fmt.Println("Error reading response from device:", err)
			return "none", err
		}

		response = bytes.TrimSpace(response)
		if len(response) > 0 {
			if f == "song" {
				fmt.Println("Received URL:", string(response))
			}
			EnhancedHls = string(response)
		} else {
			fmt.Println("Received an empty response")
		}
	}
	return EnhancedHls, nil
}

func formatAvailability(available bool, quality string) string {
	if !available {
		return "Not Available"
	}
	return quality
}

func extractMedia(b string, more_mode bool) (string, string, error) {
	masterUrl, err := url.Parse(b)
	if err != nil {
		return "", "", err
	}
	resp, err := http.Get(b)
	if err != nil {
		return "", "", err
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusOK {
		return "", "", errors.New(resp.Status)
	}
	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", "", err
	}
	masterString := string(body)
	from, listType, err := m3u8.DecodeFrom(strings.NewReader(masterString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", "", errors.New("m3u8 not of master type")
	}
	master := from.(*m3u8.MasterPlaylist)
	var streamUrl *url.URL
	sort.Slice(master.Variants, func(i, j int) bool {
		return master.Variants[i].AverageBandwidth > master.Variants[j].AverageBandwidth
	})
	if debug_mode && more_mode {
		fmt.Println("\nDebug: All Available Variants:")
		var data [][]string
		for _, variant := range master.Variants {
			data = append(data, []string{variant.Codecs, variant.Audio, fmt.Sprint(variant.Bandwidth)})
		}
		table := tablewriter.NewWriter(os.Stdout)
		table.SetHeader([]string{"Codec", "Audio", "Bandwidth"})
		table.SetAutoMergeCells(true)
		table.SetRowLine(true)
		table.AppendBulk(data)
		table.Render()

		var hasAAC, hasLossless, hasHiRes, hasAtmos, hasDolbyAudio bool
		var aacQuality, losslessQuality, hiResQuality, atmosQuality, dolbyAudioQuality string

		for _, variant := range master.Variants {
			if variant.Codecs == "mp4a.40.2" { // AAC
				hasAAC = true
				split := strings.Split(variant.Audio, "-")
				if len(split) >= 3 {
					bitrate, _ := strconv.Atoi(split[2])
					currentBitrate := 0
					if aacQuality != "" {
						current := strings.Split(aacQuality, " | ")[2]
						current = strings.Split(current, " ")[0]
						currentBitrate, _ = strconv.Atoi(current)
					}
					if bitrate > currentBitrate {
						aacQuality = fmt.Sprintf("AAC | 2 Channel | %d Kbps", bitrate)
					}
				}
			} else if variant.Codecs == "ec-3" && strings.Contains(variant.Audio, "atmos") { // Dolby Atmos
				hasAtmos = true
				split := strings.Split(variant.Audio, "-")
				if len(split) > 0 {
					bitrateStr := split[len(split)-1]
					if len(bitrateStr) == 4 && bitrateStr[0] == '2' {
						bitrateStr = bitrateStr[1:]
					}
					bitrate, _ := strconv.Atoi(bitrateStr)
					currentBitrate := 0
					if atmosQuality != "" {
						current := strings.Split(strings.Split(atmosQuality, " | ")[2], " ")[0]
						currentBitrate, _ = strconv.Atoi(current)
					}
					if bitrate > currentBitrate {
						atmosQuality = fmt.Sprintf("E-AC-3 | 16 Channel | %d Kbps", bitrate)
					}
				}
			} else if variant.Codecs == "alac" { // ALAC (Lossless or Hi-Res)
				split := strings.Split(variant.Audio, "-")
				if len(split) >= 3 {
					bitDepth := split[len(split)-1]
					sampleRate := split[len(split)-2]
					sampleRateInt, _ := strconv.Atoi(sampleRate)
					if sampleRateInt > 48000 { // Hi-Res
						hasHiRes = true
						hiResQuality = fmt.Sprintf("ALAC | 2 Channel | %s-bit/%d kHz", bitDepth, sampleRateInt/1000)
					} else { // Standard Lossless
						hasLossless = true
						losslessQuality = fmt.Sprintf("ALAC | 2 Channel | %s-bit/%d kHz", bitDepth, sampleRateInt/1000)
					}
				}
			} else if variant.Codecs == "ac-3" { // Dolby Audio
				hasDolbyAudio = true
				split := strings.Split(variant.Audio, "-")
				if len(split) > 0 {
					bitrate, _ := strconv.Atoi(split[len(split)-1])
					dolbyAudioQuality = fmt.Sprintf("AC-3 |  16 Channel | %d Kbps", bitrate)
				}
			}
		}

		fmt.Println("Available Audio Formats:")
		fmt.Println("------------------------")
		fmt.Printf("AAC             : %s\n", formatAvailability(hasAAC, aacQuality))
		fmt.Printf("Lossless        : %s\n", formatAvailability(hasLossless, losslessQuality))
		fmt.Printf("Hi-Res Lossless : %s\n", formatAvailability(hasHiRes, hiResQuality))
		fmt.Printf("Dolby Atmos     : %s\n", formatAvailability(hasAtmos, atmosQuality))
		fmt.Printf("Dolby Audio     : %s\n", formatAvailability(hasDolbyAudio, dolbyAudioQuality))
		fmt.Println("------------------------")

		return "", "", nil
	}
	var Quality string
	for _, variant := range master.Variants {
		if dl_atmos {
			if variant.Codecs == "ec-3" && strings.Contains(variant.Audio, "atmos") {
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found Dolby Atmos variant - %s (Bitrate: %d Kbps)\n",
						variant.Audio, variant.Bandwidth/1000)
				}
				split := strings.Split(variant.Audio, "-")
				length := len(split)
				length_int, err := strconv.Atoi(split[length-1])
				if err != nil {
					return "", "", err
				}
				if length_int <= Config.AtmosMax {
					if !debug_mode && !more_mode {
						fmt.Printf("%s\n", variant.Audio)
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						return "", "", err
					}
					streamUrl = streamUrlTemp
					Quality = fmt.Sprintf("%s Kbps", split[len(split)-1])
					break
				}
			} else if variant.Codecs == "ac-3" { // Add Dolby Audio support
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found Dolby Audio variant - %s (Bitrate: %d Kbps)\n",
						variant.Audio, variant.Bandwidth/1000)
				}
				streamUrlTemp, err := masterUrl.Parse(variant.URI)
				if err != nil {
					return "", "", err
				}
				streamUrl = streamUrlTemp
				split := strings.Split(variant.Audio, "-")
				Quality = fmt.Sprintf("%s Kbps", split[len(split)-1])
				break
			}
		} else if dl_aac {
			if variant.Codecs == "mp4a.40.2" {
				if debug_mode && !more_mode {
					fmt.Printf("Debug: Found AAC variant - %s (Bitrate: %d)\n", variant.Audio, variant.Bandwidth)
				}
				aacregex := regexp.MustCompile(`audio-stereo-\d+`)
				replaced := aacregex.ReplaceAllString(variant.Audio, "aac")
				if replaced == Config.AacType {
					if !debug_mode && !more_mode {
						fmt.Printf("%s\n", variant.Audio)
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						panic(err)
					}
					streamUrl = streamUrlTemp
					split := strings.Split(variant.Audio, "-")
					Quality = fmt.Sprintf("%s Kbps", split[2])
					break
				}
			}
		} else {
			if variant.Codecs == "alac" {
				split := strings.Split(variant.Audio, "-")
				length := len(split)
				length_int, err := strconv.Atoi(split[length-2])
				if err != nil {
					return "", "", err
				}
				if length_int <= Config.AlacMax {
					if !debug_mode && !more_mode {
						fmt.Printf("%s-bit / %s Hz\n", split[length-1], split[length-2])
					}
					streamUrlTemp, err := masterUrl.Parse(variant.URI)
					if err != nil {
						panic(err)
					}
					streamUrl = streamUrlTemp
					KHZ := float64(length_int) / 1000.0
					Quality = fmt.Sprintf("%sB-%.1fkHz", split[length-1], KHZ)
					break
				}
			}
		}
	}
	if streamUrl == nil {
		return "", "", errors.New("no codec found")
	}
	return streamUrl.String(), Quality, nil
}
func extractVideo(c string) (string, error) {
	MediaUrl, err := url.Parse(c)
	if err != nil {
		return "", err
	}

	resp, err := http.Get(c)
	if err != nil {
		return "", err
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return "", errors.New(resp.Status)
	}

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return "", err
	}
	videoString := string(body)

	from, listType, err := m3u8.DecodeFrom(strings.NewReader(videoString), true)
	if err != nil || listType != m3u8.MASTER {
		return "", errors.New("m3u8 not of media type")
	}

	video := from.(*m3u8.MasterPlaylist)

	re := regexp.MustCompile(`_(\d+)x(\d+)`)

	var streamUrl *url.URL
	sort.Slice(video.Variants, func(i, j int) bool {
		return video.Variants[i].AverageBandwidth > video.Variants[j].AverageBandwidth
	})

	maxHeight := Config.MVMax

	for _, variant := range video.Variants {
		matches := re.FindStringSubmatch(variant.URI)
		if len(matches) == 3 {
			height := matches[2]
			var h int
			_, err := fmt.Sscanf(height, "%d", &h)
			if err != nil {
				continue
			}
			if h <= maxHeight {
				streamUrl, err = MediaUrl.Parse(variant.URI)
				if err != nil {
					return "", err
				}
				fmt.Println("Video: " + variant.Resolution + "-" + variant.VideoRange)
				break
			}
		}
	}

	if streamUrl == nil {
		return "", errors.New("no suitable video stream found")
	}

	return streamUrl.String(), nil
}

// ======== Web API: 轻量任务管理与路由 ========

type TaskStatus string

const (
	StatusQueued    TaskStatus = "queued"
	StatusRunning   TaskStatus = "running"
	StatusSucceeded TaskStatus = "succeeded"
	StatusFailed    TaskStatus = "failed"
)

type Task struct {
	ID         string     `json:"id"`
	URL        string     `json:"url"`
	Quality    string     `json:"quality"` // alac/aac/atmos
	Status     TaskStatus `json:"status"`
	Progress   int        `json:"progress"`
	TotalUnits int        `json:"totalUnits"` // 总曲目数/阶段数
	DoneUnits  int        `json:"doneUnits"`  // 已完成曲目/阶段
	Message    string     `json:"message"`
	Logs       []string   `json:"logs"`
	Token      string     `json:"-"` // 使用到的授权 token（可热更新）
	// 选项快照（用于 runner）
	Tracks     []int `json:"-"`
    SongOnly   bool  `json:"-"`
    MaxRetries int   `json:"-"`
    CreatedAt  time.Time `json:"createdAt"`
    UpdatedAt  time.Time `json:"updatedAt"`
    Canceled   bool      `json:"canceled"`
    SubPercent int       `json:"subPercent"`
    SubMessage string    `json:"subMessage,omitempty"`
}

type TaskManager struct {
	mu      sync.RWMutex
	tasks   map[string]*Task
	queue   chan string
	workers int
	runner  func(*Task) error
}

func NewTaskManager(workers, queueSize int) *TaskManager {
	m := &TaskManager{
		tasks:   make(map[string]*Task),
		queue:   make(chan string, queueSize),
		workers: workers,
	}
	for i := 0; i < workers; i++ {
		go m.worker()
	}
	return m
}

func (m *TaskManager) BindRunner(r func(*Task) error) { m.runner = r }

func (m *TaskManager) Create(url, quality, token string) *Task {
	id := uuid.New().String()
	t := &Task{
		ID:       id,
		URL:      url,
		Quality:  quality,
		Status:   StatusQueued,
		Progress: 0,
        Logs:     []string{"queued"},
        Token:    token,
        CreatedAt: time.Now(),
        UpdatedAt: time.Now(),
    }
	m.mu.Lock()
	m.tasks[id] = t
	m.mu.Unlock()
	m.queue <- id
	return t
}

func (m *TaskManager) Get(id string) (*Task, bool) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	t, ok := m.tasks[id]
	return t, ok
}

func (m *TaskManager) List() []*Task {
	mu := &m.mu
	mu.RLock()
	defer mu.RUnlock()
	out := make([]*Task, 0, len(m.tasks))
	for _, t := range m.tasks {
		out = append(out, t)
	}
    sort.Slice(out, func(i, j int) bool {
        if out[i].CreatedAt.Equal(out[j].CreatedAt) {
            return out[i].ID < out[j].ID
        }
        return out[i].CreatedAt.Before(out[j].CreatedAt)
    })
    return out
}

func (m *TaskManager) AppendLog(id, msg string) {
    m.mu.Lock()
    if t, ok := m.tasks[id]; ok {
        t.Logs = append(t.Logs, msg)
        t.Message = msg
        t.UpdatedAt = time.Now()
        if len(t.Logs) > 1000 {
            t.Logs = t.Logs[len(t.Logs)-1000:]
        }
    }
    m.mu.Unlock()
}

// ClearCompleted 删除状态为 succeeded/failed 的任务，返回删除数量
func (m *TaskManager) ClearCompleted() int {
    m.mu.Lock()
    defer m.mu.Unlock()
    removed := 0
    for id, t := range m.tasks {
        if t.Status == StatusSucceeded || t.Status == StatusFailed {
            delete(m.tasks, id)
            removed++
        }
    }
    return removed
}

// SetSubProgress 更新子进度（当前曲目标记），仅用于运行中任务
func (m *TaskManager) SetSubProgress(id string, percent int, msg string) {
    if percent < 0 { percent = 0 }
    if percent > 100 { percent = 100 }
    m.mu.Lock()
    if t, ok := m.tasks[id]; ok {
        t.SubPercent = percent
        if msg != "" { t.SubMessage = msg }
        t.UpdatedAt = time.Now()
    }
    m.mu.Unlock()
}

// Cancel 标记任务为取消。对排队任务会直接标记失败；对运行中任务标记为取消中
func (m *TaskManager) Cancel(id string) bool {
    m.mu.Lock()
    defer m.mu.Unlock()
    if t, ok := m.tasks[id]; ok {
        t.Canceled = true
        if t.Status == StatusQueued {
            t.Status = StatusFailed
            t.Message = "canceled"
            t.Logs = append(t.Logs, "canceled")
        } else if t.Status == StatusRunning {
            t.Message = "canceling"
            t.Logs = append(t.Logs, "cancel requested")
        }
        t.UpdatedAt = time.Now()
        return true
    }
    return false
}

// Delete 删除一个非运行中的任务（包括排队/已完成/失败）。运行中返回 false
func (m *TaskManager) Delete(id string) bool {
    m.mu.Lock()
    defer m.mu.Unlock()
    if t, ok := m.tasks[id]; ok {
        if t.Status == StatusRunning {
            return false
        }
        delete(m.tasks, id)
        return true
    }
    return false
}

func (m *TaskManager) setStatus(t *Task, s TaskStatus, msg string) {
    m.mu.Lock()
    t.Status = s
    t.Message = msg
    if msg != "" {
        t.Logs = append(t.Logs, msg)
    }
    t.UpdatedAt = time.Now()
    if len(t.Logs) > 1000 {
        t.Logs = t.Logs[len(t.Logs)-1000:]
    }
    m.mu.Unlock()
}

func (m *TaskManager) worker() {
    for id := range m.queue {
        m.mu.RLock()
        t := m.tasks[id]
        m.mu.RUnlock()
        if t == nil {
            continue
        }
        // 若已被取消，直接标记并跳过
        if t.Canceled {
            m.setStatus(t, StatusFailed, "canceled")
            continue
        }
        m.setStatus(t, StatusRunning, "started")
        if t.Canceled {
            m.setStatus(t, StatusFailed, "canceled")
            continue
        }
        if m.runner == nil {
            m.setStatus(t, StatusFailed, "runner not bound")
            continue
        }
        if err := m.runner(t); err != nil {
            m.setStatus(t, StatusFailed, err.Error())
        } else {
            m.setStatus(t, StatusSucceeded, "done")
        }
    }
}

type createTaskReq struct {
	URLs       []string `json:"urls"`
	URL        string   `json:"url,omitempty"`
	Quality    string   `json:"quality" binding:"required,oneof=alac aac atmos"`
	Tracks     []int    `json:"tracks,omitempty"`     // 选曲，如 [1,3,5]；空=全下
	SongOnly   bool     `json:"songOnly,omitempty"`   // 单曲模式（等价 CLI 的 --song）
	MaxRetries int      `json:"maxRetries,omitempty"` // 自动重试次数（可选，默认 1）
}

func registerRoutes(r *gin.Engine, mgr *TaskManager, token string) {
    v1 := r.Group("/v1")
    {
        // 清空下载目录：删除 Config 中配置的三个保存目录内的所有内容
        v1.POST("/cleanup-downloads", func(c *gin.Context) {
            // 若存在运行中的任务，拒绝清理
            mgr.mu.RLock()
            running := false
            for _, t := range mgr.tasks {
                if t.Status == StatusRunning {
                    running = true
                    break
                }
            }
            mgr.mu.RUnlock()
            if running {
                c.JSON(http.StatusConflict, gin.H{"error": "有任务仍在运行，无法清空下载目录"})
                return
            }

            base, err := os.Getwd()
            if err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }

            // 规范化 base，用于子路径校验
            baseAbs, err := filepath.Abs(base)
            if err != nil {
                c.JSON(http.StatusInternalServerError, gin.H{"error": err.Error()})
                return
            }
            if !strings.HasSuffix(baseAbs, string(os.PathSeparator)) {
                baseAbs = baseAbs + string(os.PathSeparator)
            }

            folders := []string{Config.AlacSaveFolder, Config.AacSaveFolder, Config.AtmosSaveFolder}
            type stat struct{ Files, Dirs int }
            total := stat{}
            cleaned := make([]gin.H, 0, len(folders))
            for _, name := range folders {
                dir := filepath.Join(baseAbs, name)
                dirAbs, _ := filepath.Abs(dir)
                // 子路径安全校验
                if !strings.HasPrefix(dirAbs+string(os.PathSeparator), baseAbs) && dirAbs != strings.TrimSuffix(baseAbs, string(os.PathSeparator)) {
                    cleaned = append(cleaned, gin.H{"folder": name, "error": "路径不安全，已跳过"})
                    continue
                }
                info, err := os.Stat(dirAbs)
                if err != nil {
                    if os.IsNotExist(err) {
                        cleaned = append(cleaned, gin.H{"folder": name, "skipped": true, "reason": "不存在"})
                        continue
                    }
                    cleaned = append(cleaned, gin.H{"folder": name, "error": err.Error()})
                    continue
                }
                if !info.IsDir() {
                    cleaned = append(cleaned, gin.H{"folder": name, "error": "不是目录"})
                    continue
                }
                entries, err := os.ReadDir(dirAbs)
                if err != nil {
                    cleaned = append(cleaned, gin.H{"folder": name, "error": err.Error()})
                    continue
                }
                cur := stat{}
                for _, e := range entries {
                    p := filepath.Join(dirAbs, e.Name())
                    if err := os.RemoveAll(p); err != nil {
                        cleaned = append(cleaned, gin.H{"folder": name, "item": e.Name(), "error": err.Error()})
                        continue
                    }
                    if e.IsDir() {
                        cur.Dirs++
                    } else {
                        cur.Files++
                    }
                }
                total.Files += cur.Files
                total.Dirs += cur.Dirs
                cleaned = append(cleaned, gin.H{"folder": name, "deletedFiles": cur.Files, "deletedDirs": cur.Dirs})
            }
            c.JSON(http.StatusOK, gin.H{
                "deletedFiles": total.Files,
                "deletedDirs":  total.Dirs,
                "folders":      cleaned,
            })
        })

        // 关键词搜索（album|song|artist）
        v1.GET("/search", func(c *gin.Context) {
            q := strings.TrimSpace(c.Query("q"))
            typ := strings.TrimSpace(c.Query("type"))
            if q == "" || (typ != "album" && typ != "song" && typ != "artist") {
                c.JSON(http.StatusBadRequest, gin.H{"error": "q and type=album|song|artist required"})
                return
            }
            limitStr := c.DefaultQuery("limit", "20")
            offsetStr := c.DefaultQuery("offset", "0")
            limit, _ := strconv.Atoi(limitStr)
            offset, _ := strconv.Atoi(offsetStr)
            if limit <= 0 || limit > 50 { limit = 20 }
            if offset < 0 { offset = 0 }

            apiType := typ + "s"
            sr, err := ampapi.Search(Config.Storefront, q, apiType, Config.Language, token, limit, offset)
            if err != nil {
                c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
                return
            }
            out := make([]gin.H, 0, limit)
            switch typ {
            case "album":
                if sr.Results.Albums != nil {
                    for _, it := range sr.Results.Albums.Data {
                        year := ""
                        if len(it.Attributes.ReleaseDate) >= 4 { year = it.Attributes.ReleaseDate[:4] }
                        out = append(out, gin.H{
                            "id": it.ID,
                            "name": it.Attributes.Name,
                            "artist": it.Attributes.ArtistName,
                            "year": year,
                            "tracks": it.Attributes.TrackCount,
                            "url": it.Attributes.URL,
                        })
                    }
                }
            case "song":
                if sr.Results.Songs != nil {
                    for _, it := range sr.Results.Songs.Data {
                        out = append(out, gin.H{
                            "id": it.ID,
                            "name": it.Attributes.Name,
                            "artist": it.Attributes.ArtistName,
                            "album": it.Attributes.AlbumName,
                            "url": it.Attributes.URL,
                        })
                    }
                }
            case "artist":
                if sr.Results.Artists != nil {
                    for _, it := range sr.Results.Artists.Data {
                        genres := ""
                        if len(it.Attributes.GenreNames) > 0 { genres = strings.Join(it.Attributes.GenreNames, ", ") }
                        out = append(out, gin.H{
                            "id": it.ID,
                            "name": it.Attributes.Name,
                            "genres": genres,
                            "url": it.Attributes.URL,
                        })
                    }
                }
            }
            c.JSON(http.StatusOK, gin.H{"items": out})
        })
		// 创建任务（支持 urls 批量，也兼容单个 url）
		v1.POST("/tasks", func(c *gin.Context) {
			var req createTaskReq
			if err := c.ShouldBindJSON(&req); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}

			// 兼容：如果老字段 url 传了而 urls 为空，就塞到 urls 里
			if len(req.URLs) == 0 && strings.TrimSpace(req.URL) != "" {
				req.URLs = []string{strings.TrimSpace(req.URL)}
			}

			// 若 urls 只有一条且原始 url 字段包含分隔符，则按换行/逗号/空白拆分覆盖
			if len(req.URLs) == 1 && strings.TrimSpace(req.URL) != "" {
				raw := strings.TrimSpace(req.URL)
				if strings.ContainsAny(raw, ",\n\r\t ") {
					parts := regexp.MustCompile(`[\,\n\r\t\s]+`).Split(raw, -1)
					req.URLs = req.URLs[:0]
					for _, p := range parts {
						u := strings.TrimSpace(p)
						if u != "" {
							req.URLs = append(req.URLs, u)
						}
					}
				}
			}

			// 校验：若 urls 为空，尝试从 url 拆分多条
			if len(req.URLs) == 0 {
				if s := strings.TrimSpace(req.URL); s != "" {
					parts := regexp.MustCompile(`[\,\n\r\t\s]+`).Split(s, -1)
					for _, p := range parts {
						u := strings.TrimSpace(p)
						if u != "" {
							req.URLs = append(req.URLs, u)
						}
					}
				}
				if len(req.URLs) == 0 {
					c.JSON(http.StatusBadRequest, gin.H{"error": "urls required"})
					return
				}
			}
			if req.Quality == "" {
				req.Quality = "alac"
			}
			switch req.Quality {
			case "alac", "aac", "atmos":
			default:
				c.JSON(http.StatusBadRequest, gin.H{"error": "quality must be one of: alac|aac|atmos"})
				return
			}
			if req.MaxRetries < 0 {
				req.MaxRetries = 0
			} else if req.MaxRetries == 0 {
				req.MaxRetries = 3 // 默认自动重试 3 次
			}

			ids := make([]string, 0, len(req.URLs))
			for _, raw := range req.URLs {
				u := strings.TrimSpace(raw)
				if u == "" {
					continue
				}
				// 创建任务（确保你的 mgr.Create 返回 *Task）
				t := mgr.Create(u, req.Quality, token)

				// 把可选字段塞进任务，供 Runner 使用
				mgr.mu.Lock()
				t.Tracks = append([]int(nil), req.Tracks...)
				t.SongOnly = req.SongOnly
				t.MaxRetries = req.MaxRetries
				t.Token = token // Runner 用 t.Token
				mgr.mu.Unlock()

				ids = append(ids, t.ID)
			}

			if len(ids) == 0 {
				c.JSON(http.StatusBadRequest, gin.H{"error": "no valid urls"})
				return
			}

			c.JSON(http.StatusCreated, gin.H{
				"taskIds": ids,
				"count":   len(ids),
			})
		})

        // 列表
        v1.GET("/tasks", func(c *gin.Context) {
            c.JSON(http.StatusOK, mgr.List())
        })

        // 清除已完成/失败的任务记录
        v1.DELETE("/tasks/completed", func(c *gin.Context) {
            n := mgr.ClearCompleted()
            c.JSON(http.StatusOK, gin.H{"deleted": n})
        })

        // 详情
        v1.GET("/tasks/:id", func(c *gin.Context) {
            id := c.Param("id")
            if t, ok := mgr.Get(id); ok {
                c.JSON(http.StatusOK, t)
                return
            }
            c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
        })

        // 删除单个任务（运行中返回 409）
        v1.DELETE("/tasks/:id", func(c *gin.Context) {
            id := c.Param("id")
            if ok := mgr.Delete(id); ok {
                c.Status(http.StatusNoContent)
                return
            }
            // 区分不存在与运行中
            if _, exist := mgr.Get(id); exist {
                c.JSON(http.StatusConflict, gin.H{"error": "task is running"})
            } else {
                c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
            }
        })

        // 取消任务（排队=直接取消；运行中=请求取消）
        v1.POST("/tasks/:id/cancel", func(c *gin.Context) {
            id := c.Param("id")
            if ok := mgr.Cancel(id); ok {
                c.JSON(http.StatusAccepted, gin.H{"status": "cancel requested"})
                return
            }
            c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
        })

		// 手动重试：将失败/成功/已完成的任务重置并重新入队
		v1.POST("/tasks/:id/retry", func(c *gin.Context) {
			id := c.Param("id")
			mgr.mu.Lock()
			t, ok := mgr.tasks[id]
			if ok {
				// 不允许对正在运行的任务进行重试
				if t.Status == StatusRunning {
					mgr.mu.Unlock()
					c.JSON(http.StatusConflict, gin.H{"error": "task is running"})
					return
				}
				// 重置状态并入队
				t.Status = StatusQueued
				t.Progress = 0
				t.DoneUnits = 0
				t.TotalUnits = 0
				t.Message = "manual retry"
				t.Logs = append(t.Logs, "manual retry")
				mgr.mu.Unlock()
				mgr.queue <- id
				c.JSON(http.StatusAccepted, gin.H{"status": "requeued"})
				return
			}
			mgr.mu.Unlock()
			c.JSON(http.StatusNotFound, gin.H{"error": "not found"})
		})

		// 单任务 SSE 进度（简单实现：每秒推送一次快照，直到结束/断开）
		v1.GET("/tasks/:id/stream", func(c *gin.Context) {
			id := c.Param("id")
			c.Writer.Header().Set("Content-Type", "text/event-stream")
			c.Writer.Header().Set("Cache-Control", "no-cache")
			c.Writer.Header().Set("Connection", "keep-alive")
			c.Writer.Header().Set("X-Accel-Buffering", "no")
			flusher, ok := c.Writer.(http.Flusher)
			if !ok {
				c.Status(http.StatusInternalServerError)
				return
			}
			ctx := c.Request.Context()
			for {
				select {
				case <-ctx.Done():
					return
				case <-time.After(1 * time.Second):
					mgr.mu.RLock()
					t, ok := mgr.tasks[id]
					if !ok {
						mgr.mu.RUnlock()
						fmt.Fprintf(c.Writer, "event: end\n")
						fmt.Fprintf(c.Writer, "data: {\"error\":\"not found\"}\n\n")
						flusher.Flush()
						return
					}
					// 复制快照，避免竞态
					snap := *t
					mgr.mu.RUnlock()
					b, _ := json.Marshal(snap)
					fmt.Fprintf(c.Writer, "data: %s\n\n", string(b))
					flusher.Flush()

					if snap.Status == StatusSucceeded || snap.Status == StatusFailed {
						fmt.Fprintf(c.Writer, "event: end\n")
						fmt.Fprintf(c.Writer, "data: {\"status\":\"%s\"}\n\n", snap.Status)
						flusher.Flush()
						return
					}
				}
			}
		})

		// 元数据：根据专辑链接返回曲目列表，方便 Web 端选曲
		v1.GET("/meta/album", func(c *gin.Context) {
			urlRaw := strings.TrimSpace(c.Query("url"))
			if urlRaw == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "url required"})
				return
			}
			storefront, albumId := checkUrl(urlRaw)
			if storefront == "" || albumId == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid album url"})
				return
			}
			album := task.NewAlbum(storefront, albumId)
			if err := album.GetResp(token, Config.Language); err != nil {
				c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
				return
			}
			meta := album.Resp
			tracks := make([]gin.H, 0, len(meta.Data[0].Relationships.Tracks.Data))
			for i, tr := range meta.Data[0].Relationships.Tracks.Data {
				tracks = append(tracks, gin.H{
					"index": i + 1,
					"id":    tr.ID,
					"name":  tr.Attributes.Name,
				})
			}
			c.JSON(http.StatusOK, gin.H{
				"title":  meta.Data[0].Attributes.Name,
				"artist": meta.Data[0].Attributes.ArtistName,
				"cover":  meta.Data[0].Attributes.Artwork.URL,
				"tracks": tracks,
			})
		})

		// 元数据：根据艺术家链接返回其专辑列表（时间升序）
		v1.GET("/meta/artist", func(c *gin.Context) {
			artistUrl := strings.TrimSpace(c.Query("url"))
			if artistUrl == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "url required"})
				return
			}
			storefront, artistId := checkUrlArtist(artistUrl)
			if storefront == "" || artistId == "" {
				c.JSON(http.StatusBadRequest, gin.H{"error": "invalid artist url"})
				return
			}
			// 直接调 Apple API 获取专辑列表，简化为 100/批 的分页
			options := make([][]string, 0)
			offset := 0
			for {
				req, err := http.NewRequest("GET", fmt.Sprintf("https://amp-api.music.apple.com/v1/catalog/%s/artists/%s/albums?limit=100&offset=%d&l=%s", storefront, artistId, offset, Config.Language), nil)
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
					return
				}
				req.Header.Set("Authorization", fmt.Sprintf("Bearer %s", token))
				req.Header.Set("User-Agent", "Mozilla/5.0")
				req.Header.Set("Origin", "https://music.apple.com")
				do, err := http.DefaultClient.Do(req)
				if err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
					return
				}
				defer do.Body.Close()
				if do.StatusCode != http.StatusOK {
					c.JSON(http.StatusBadRequest, gin.H{"error": do.Status})
					return
				}
				obj := new(structs.AutoGeneratedArtist)
				if err := json.NewDecoder(do.Body).Decode(&obj); err != nil {
					c.JSON(http.StatusBadRequest, gin.H{"error": err.Error()})
					return
				}
				for _, album := range obj.Data {
					options = append(options, []string{album.Attributes.Name, album.Attributes.ReleaseDate, album.ID, album.Attributes.URL})
				}
				if len(obj.Next) == 0 {
					break
				}
				offset += 100
			}
			sort.Slice(options, func(i, j int) bool {
				dateI, _ := time.Parse("2006-01-02", options[i][1])
				dateJ, _ := time.Parse("2006-01-02", options[j][1])
				return dateI.Before(dateJ)
			})
			out := make([]gin.H, 0, len(options))
			for _, v := range options {
				out = append(out, gin.H{
					"name": v[0],
					"date": v[1],
					"id":   v[2],
					"url":  v[3],
				})
			}
			c.JSON(http.StatusOK, gin.H{"albums": out})
		})
	}
}
