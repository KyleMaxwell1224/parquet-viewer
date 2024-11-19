use eframe::egui;
use crate::gui::GuiState;
use crate::datafusion_query::{execute_query, RowData};
use std::sync::{Arc, Mutex};

pub struct ParquetApp {
    gui_state: GuiState,
    data: Arc<Mutex<Vec<RowData>>>,
}

impl ParquetApp {
    pub fn new() -> Self {
        Self {
            gui_state: GuiState::default(),
            data: Arc::new(Mutex::new(Vec::new())),
        }
    }
}

impl eframe::App for ParquetApp {
    fn update(&mut self, ctx: &egui::Context, _frame: &mut eframe::Frame) {
        self.gui_state.update(ctx);
    }
}