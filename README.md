# GRUPO LOS COVID
## INTEGRANTES
- Chavez Cruz Jhunior Kenyi
- Huillca Roque Ruth Esther
- Salcedo Almirón Melvin
- Visa Flores Alberto

## Pruebas para llenado de formulario, segunda fase
- [LINK DE VIDEO] ![](https://drive.google.com/file/d/1yzLkW2kYfwd4ww76Yt96tFUIB5sHBhg9/view?usp=sharing)

### Pruebas de de Ajuste de coseno

### Pruebas de Slope 
#### Prueba 1 agregado y prediccions de un nuevo usuario
- La prueba se puede ver ![aquí](https://github.com/MelvinSalcedo/topicosBaseDatosCompartida/blob/master/Formulario%20Pruebas/SlopeO-20usuarios-prediccion.txt)


```console
Nuevo usuario creado
Id del nuevo usuario->162542

1) Busqueda
2) Prediccion
3) Nuevo Usuario
4) Iniciar Sesion
5) Recomendacion1
6) Recomendacion2
7) Recomendacion3
Opcion->4

UserId->162542
Bienvenido usuario 162542
1) Mis ranqueados
2) Ranquear pelicula
3) Cerrar Sesion
Opcion->1

.
.
.
UserId->162542
MovieId->1
El usuario 162541 pondría el puntaje 3.67956 al libro Toy Story (1995)
0.324024s

1) Busqueda
2) Prediccion
3) Nuevo Usuario
4) Iniciar Sesion
5) Recomendacion1
6) Recomendacion2
7) Recomendacion3
Opcion->2

UserId->162542
MovieId->100
El usuario 162541 pondría el puntaje 3.30432 al libro City Hall (1996)
0.219335s
 ```
#### Como un nuevo usuario altera predicciones
- La prueba se puede ver ![aquí](https://github.com/MelvinSalcedo/topicosBaseDatosCompartida/blob/master/Formulario%20Pruebas/SlopeO-predictusermodified.txt)




```console
UserId->50
MovieId->80
El usuario 49 pondría el puntaje 4.43579 al libro "White Balloon
1.1643s
1) Busqueda
2) Prediccion
3) Nuevo Usuario
4) Iniciar Sesion
5) Recomendacion1
6) Recomendacion2
7) Recomendacion3
Opcion->4

UserId->162542
Bienvenido usuario 162542
1) Mis ranqueados
2) Ranquear pelicula
3) Cerrar Sesion
Opcion->2

MovieId->80
Puntaje->5
Pelicula ranqueada correctamente

1) Mis ranqueados
2) Ranquear pelicula
3) Cerrar Sesion
Opcion->3


1) Busqueda
2) Prediccion
3) Nuevo Usuario
4) Iniciar Sesion
5) Recomendacion1
6) Recomendacion2
7) Recomendacion3
Opcion->2

UserId->50
MovieId->80
El usuario 49 pondría el puntaje 4.43615 al libro "White Balloon
1.21455s
```

 
