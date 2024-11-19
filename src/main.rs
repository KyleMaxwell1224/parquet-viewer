mod app;
mod gui;
mod datafusion_query;

use app::ParquetApp;

#[tokio::main]
async fn main() {
    // Initialize the GUI application
    let options = eframe::NativeOptions::default();
    eframe::run_native(
        "Parquet Viewer",
        options,
        Box::new(|_cc| Box::new(ParquetApp::new())),
    );
}