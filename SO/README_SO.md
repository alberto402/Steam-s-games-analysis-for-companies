
Requisitos:
  - Tener instalado spark 
  - Tener descargado fichero de datos games-features.csv
  
Antes de ejecutar:
  - Revisar los .py, cambiar "ruta" por el path donde se encuentren los *.csv
  
Orden de ejecución:
  1) disponibles_so.py
      - Crea el fichero count_fun.csv, cuántos juegos funcionan en cada plataforma
      - Crea los ficheros win_genre.csv, lin_genre.csv y mac_genre.csv para map-reduce de categorías
      - Crea los ficheros win_categ.csv, lin_categ.csv y mac_categ.csv para map-reduce de géneros
  2) requisitos_so.py
      - Crea el fichero count_req.csv, cuántos juegos piden requisitos mínimos en cada plataforma
  3) mapper.py y reducer.py
      - cat mac_genre.csv | ./mapper.py | sort | ./reducer.py "Mac" "Genre" > mac_genre_count.csv
      - cat win_genre.csv | ./mapper.py | sort | ./reducer.py "Win" "Genre" > win_genre_count.csv
      - cat lin_genre.csv | ./mapper.py | sort | ./reducer.py "Linux" "Genre" > lin_genre_count.csv
      - Para los de categorías igual pero con "categ" en lugar de "genre"
  4) n_juegos_x_categoria.py
      - Crea el fichero category_so.csv, número de juegos de *x* género en y plataforma
  5) n_juegos_x_genero.py
      - Crea el fichero genre_so.csv, Número de juegos de *x* categoría en *y* plataforma
