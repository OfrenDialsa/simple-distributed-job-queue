package htmx

import (
	"html/template"
	"io"
	"os"
	"path/filepath"

	"github.com/labstack/echo/v4"
)

type TemplateRenderer struct {
	templates *template.Template
}

func NewTemplateRenderer(rootDir string) *TemplateRenderer {
	var files []string

	// Walk menjelajahi folder web/htmx beserta seluruh sub-folder (termasuk fragments)
	err := filepath.Walk(rootDir, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		// Ambil semua file dengan ekstensi .html
		if !info.IsDir() && filepath.Ext(path) == ".html" {
			files = append(files, path)
		}
		return nil
	})

	if err != nil || len(files) == 0 {
		panic("Failed to find HTMX template files in: " + rootDir)
	}

	// Parse seluruh file .html yang ditemukan ke dalam 1 instance template
	return &TemplateRenderer{
		templates: template.Must(template.ParseFiles(files...)),
	}
}

func (t *TemplateRenderer) Render(w io.Writer, name string, data interface{}, c echo.Context) error {
	return t.templates.ExecuteTemplate(w, name, data)
}
