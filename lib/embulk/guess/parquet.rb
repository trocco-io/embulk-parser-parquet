Embulk::JavaPlugin.register_guess(
  "parquet", "org.embulk.guess.parquet.ParquetGuessPlugin",
  File.expand_path("../../../../classpath", __FILE__))
