use eframe::egui::{self, RichText};
use crate::datafusion_query::{execute_query, RowData};
use rfd::FileDialog;
use std::sync::{Arc, Mutex};
use poll_promise::Promise;

#[derive(Default)]
pub struct GuiState {
    pub file_path: String,
    pub error_message: Option<String>,
    pub query: String,
    pub headers: Vec<String>,
    pub current_page: usize,
    pub page_size: usize,
    pub total_pages: usize,
    pub loading: bool,
    pub data_promise: Option<Promise<Result<Vec<RowData>, String>>>,
    pub data: Vec<RowData>,
}

impl GuiState {
    pub fn update(&mut self, ctx: &egui::Context) {
        // Top panel for file selection and query input
        egui::TopBottomPanel::top("top_panel").show(ctx, |ui| {
            ui.horizontal(|ui| {
                if ui.button("Open File").clicked() && !self.loading {
                    if let Some(path) = FileDialog::new().pick_file() {
                        self.file_path = path.display().to_string();
                        self.query = "SELECT * FROM parquet_table LIMIT 100".to_string();
                        self.current_page = 0;
                        self.load_data();
                    }
                }
                ui.label(&self.file_path);
            });

            ui.separator();

            ui.horizontal(|ui| {
                ui.label("SQL Query:");
                ui.text_edit_singleline(&mut self.query);
                if ui.button("Run Query").clicked() && !self.loading {
                    self.current_page = 0;
                    self.load_data();
                }
            });

            if let Some(ref msg) = self.error_message {
                ui.colored_label(egui::Color32::RED, msg);
            }
        });

        // Check if the data is ready
        if let Some(promise) = &self.data_promise {
            if let Some(result) = promise.ready() {
                self.loading = false;
                match result {
                    Ok(results) => {
                        self.data = results.clone();
                        if let Some(first_row) = self.data.first() {
                            self.headers = first_row.headers.clone();
                        }
                        self.page_size = 100; // Set page size
                        self.total_pages = (self.data.len() + self.page_size - 1) / self.page_size;
                        self.error_message = None;
                    }
                    Err(e) => {
                        self.error_message = Some(format!("Error: {}", e));
                    }
                }
                self.data_promise = None;
            } else {
                self.loading = true;
            }
        }

        // Main panel for displaying data
        egui::CentralPanel::default().show(ctx, |ui| {
            if self.loading {
                ui.centered_and_justified(|ui| {
                    ui.add(egui::widgets::Spinner::new());
                    ui.label("Loading...");
                });
            } else if !self.data.is_empty() {
                let start_index = self.current_page * self.page_size;
                let end_index = usize::min(start_index + self.page_size, self.data.len());
                let current_page_data = &self.data[start_index..end_index];

                egui::ScrollArea::both().show(ui, |ui| {
                    egui::Grid::new("data_grid")
                        .striped(true)
                        .show(ui, |ui| {
                            // Display headers
                            for header in &self.headers {
                                ui.heading(header);
                            }
                            ui.end_row();

                            // Display rows
                            for row in current_page_data {
                                for value in &row.values {
                                    ui.label(value);
                                }
                                ui.end_row();
                            }
                        });
                });

                // Pagination controls
                ui.horizontal(|ui| {
                    if ui.button("Previous").clicked() && self.current_page > 0 {
                        self.current_page -= 1;
                    }
                    if ui.button("Next").clicked() && self.current_page + 1 < self.total_pages {
                        self.current_page += 1;
                    }
                    ui.label(format!(
                        "Page {}/{}",
                        self.current_page + 1,
                        self.total_pages
                    ));
                });
            } else {
                ui.label("No data loaded.");
            }
        });
    }

    pub fn load_data(&mut self) {
        let file_path = self.file_path.clone();
        let query = self.query.clone();

        self.loading = true;
        self.error_message = None;

        // Create a promise to handle the async task
        let (sender, promise) = Promise::new();

        // Store the promise in the state
        self.data_promise = Some(promise);

        // Spawn the async task
        std::thread::spawn(move || {
            let rt = tokio::runtime::Runtime::new().unwrap();
            let result = rt.block_on(async move {
                execute_query(&file_path, &query)
                    .await
                    .map_err(|e| format!("{}", e))
            });
            sender.send(result);
        });
    }
}