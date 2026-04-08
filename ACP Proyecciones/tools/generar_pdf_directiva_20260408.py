from __future__ import annotations

from pathlib import Path
import textwrap


ORIGEN_MD = Path(
    r"D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\Avance\INFORME_EJECUTIVO_DIRECTIVA_20260408.md"
)
DESTINO_PDF = Path(
    r"D:\Proyecto2026\ACP_DWH\ACP Proyecciones\ETL\Avance\INFORME_EJECUTIVO_DIRECTIVA_20260408.pdf"
)

ANCHO = 595
ALTO = 842
MARGEN_X = 54
MARGEN_Y = 56

COLORES = {
    "verde_acp": (0.12, 0.42, 0.21),
    "verde_oscuro": (0.08, 0.28, 0.15),
    "verde_claro": (0.87, 0.93, 0.89),
    "bronce": (0.72, 0.50, 0.24),
    "arena": (0.96, 0.95, 0.92),
    "gris_texto": (0.20, 0.22, 0.24),
    "gris_suave": (0.42, 0.46, 0.49),
    "blanco": (1.0, 1.0, 1.0),
}

FUENTES = {
    "regular": ("F1", "Helvetica"),
    "bold": ("F2", "Helvetica-Bold"),
    "italic": ("F3", "Helvetica-Oblique"),
}


def escapar_pdf(texto: str) -> str:
    return (
        texto.replace("\\", r"\\")
        .replace("(", r"\(")
        .replace(")", r"\)")
        .replace("\r", " ")
        .replace("\n", " ")
    )


class PaginaPdf:
    def __init__(self) -> None:
        self.comandos: list[str] = []
        self.y = ALTO - MARGEN_Y

    def color_relleno(self, color: tuple[float, float, float]) -> None:
        r, g, b = color
        self.comandos.append(f"{r:.3f} {g:.3f} {b:.3f} rg")

    def color_trazo(self, color: tuple[float, float, float]) -> None:
        r, g, b = color
        self.comandos.append(f"{r:.3f} {g:.3f} {b:.3f} RG")

    def rectangulo(
        self,
        x: float,
        y: float,
        w: float,
        h: float,
        color_relleno: tuple[float, float, float] | None = None,
        color_trazo: tuple[float, float, float] | None = None,
        grosor: float = 1,
    ) -> None:
        if color_relleno is not None:
            self.color_relleno(color_relleno)
        if color_trazo is not None:
            self.color_trazo(color_trazo)
        self.comandos.append(f"{grosor:.2f} w {x:.2f} {y:.2f} {w:.2f} {h:.2f} re")
        if color_relleno is not None and color_trazo is not None:
            self.comandos.append("B")
        elif color_relleno is not None:
            self.comandos.append("f")
        else:
            self.comandos.append("S")

    def linea(
        self,
        x1: float,
        y1: float,
        x2: float,
        y2: float,
        color: tuple[float, float, float],
        grosor: float = 1,
    ) -> None:
        self.color_trazo(color)
        self.comandos.append(f"{grosor:.2f} w {x1:.2f} {y1:.2f} m {x2:.2f} {y2:.2f} l S")

    def texto(
        self,
        x: float,
        y: float,
        texto: str,
        fuente: str = "regular",
        tam: int = 11,
        color: tuple[float, float, float] | None = None,
    ) -> None:
        ref, _ = FUENTES[fuente]
        if color is not None:
            self.color_relleno(color)
        self.comandos.append(
            f"BT /{ref} {tam} Tf {x:.2f} {y:.2f} Td ({escapar_pdf(texto)}) Tj ET"
        )

    def necesita_espacio(self, alto: float) -> bool:
        return self.y - alto < MARGEN_Y + 30


def envolver(texto: str, ancho: int) -> list[str]:
    return textwrap.wrap(
        texto,
        width=ancho,
        break_long_words=False,
        break_on_hyphens=False,
    ) or [""]


def leer_bloques_md() -> list[tuple[str, str]]:
    bloques: list[tuple[str, str]] = []
    for linea in ORIGEN_MD.read_text(encoding="utf-8").splitlines():
        limpia = linea.strip()
        if not limpia:
            bloques.append(("blank", ""))
        elif limpia.startswith("# "):
            continue
        elif limpia.startswith("## "):
            bloques.append(("h2", limpia[3:].strip()))
        elif limpia.startswith("### "):
            bloques.append(("h3", limpia[4:].strip()))
        elif limpia.startswith("- "):
            bloques.append(("li", limpia[2:].strip()))
        else:
            bloques.append(("p", limpia))
    return bloques


paginas: list[PaginaPdf] = []


def nueva_pagina() -> PaginaPdf:
    pagina = PaginaPdf()
    paginas.append(pagina)
    return pagina


def encabezado_contenido(pagina: PaginaPdf) -> None:
    pagina.rectangulo(0, ALTO - 66, ANCHO, 66, color_relleno=COLORES["verde_oscuro"])
    pagina.rectangulo(0, ALTO - 74, ANCHO, 8, color_relleno=COLORES["bronce"])
    pagina.texto(
        MARGEN_X,
        ALTO - 42,
        "ACP PROYECCIONES | INFORME EJECUTIVO",
        fuente="bold",
        tam=12,
        color=COLORES["blanco"],
    )
    pagina.linea(MARGEN_X, 48, ANCHO - MARGEN_X, 48, COLORES["verde_claro"], grosor=0.8)


def pie_pagina(pagina: PaginaPdf, numero: int) -> None:
    pagina.texto(
        MARGEN_X,
        28,
        "Proyecto ACP Proyecciones",
        fuente="italic",
        tam=9,
        color=COLORES["gris_suave"],
    )
    pagina.texto(
        ANCHO - MARGEN_X - 50,
        28,
        f"Pagina {numero}",
        fuente="italic",
        tam=9,
        color=COLORES["gris_suave"],
    )


def agregar_portada() -> None:
    p = nueva_pagina()
    p.rectangulo(0, 0, ANCHO, ALTO, color_relleno=COLORES["arena"])
    p.rectangulo(0, ALTO - 240, ANCHO, 240, color_relleno=COLORES["verde_oscuro"])
    p.rectangulo(0, ALTO - 254, ANCHO, 14, color_relleno=COLORES["bronce"])
    p.rectangulo(ANCHO - 150, 0, 150, ALTO, color_relleno=(0.93, 0.90, 0.84))
    p.rectangulo(MARGEN_X, 360, 360, 160, color_relleno=COLORES["blanco"], color_trazo=(0.88, 0.88, 0.88), grosor=0.8)

    p.texto(
        MARGEN_X,
        ALTO - 110,
        "INFORME EJECUTIVO",
        fuente="bold",
        tam=26,
        color=COLORES["blanco"],
    )
    p.texto(
        MARGEN_X,
        ALTO - 145,
        "ACP Proyecciones",
        fuente="bold",
        tam=19,
        color=COLORES["blanco"],
    )
    p.texto(
        MARGEN_X,
        ALTO - 178,
        "Avance general del proyecto de datos",
        fuente="regular",
        tam=14,
        color=(0.92, 0.96, 0.93),
    )
    p.texto(
        MARGEN_X,
        ALTO - 212,
        "Fecha de corte: 08 de abril de 2026",
        fuente="regular",
        tam=11,
        color=(0.92, 0.96, 0.93),
    )

    resumen = [
        "El proyecto paso de una operacion muy manual a una plataforma mucho mas ordenada.",
        "Hoy existe mejor trazabilidad para entender que paso con cada carga.",
        "Los pendientes estan mas claros y mas separados entre temas tecnicos y de negocio.",
        "La base ya permite trabajar con una mirada mas confiable para seguimiento y proyecciones.",
    ]
    y = 492
    p.texto(MARGEN_X + 22, y, "MENSAJES CLAVE", fuente="bold", tam=13, color=COLORES["verde_oscuro"])
    y -= 28
    for item in resumen:
        p.rectangulo(MARGEN_X + 22, y - 4, 8, 8, color_relleno=COLORES["bronce"])
        lineas = envolver(item, 56)
        p.texto(MARGEN_X + 42, y - 2, lineas[0], fuente="regular", tam=11, color=COLORES["gris_texto"])
        y -= 16
        for extra in lineas[1:]:
            p.texto(MARGEN_X + 42, y - 2, extra, fuente="regular", tam=11, color=COLORES["gris_texto"])
            y -= 16
        y -= 10

    p.texto(
        MARGEN_X,
        60,
        "Documento ejecutivo para revision directiva",
        fuente="italic",
        tam=10,
        color=COLORES["gris_suave"],
    )


def agregar_pagina_mensajes() -> None:
    p = nueva_pagina()
    encabezado_contenido(p)
    p.y = ALTO - 110
    p.texto(MARGEN_X, p.y, "Mensajes principales", fuente="bold", tam=20, color=COLORES["verde_oscuro"])
    p.y -= 24
    p.texto(
        MARGEN_X,
        p.y,
        "Esta pagina resume, de forma muy ejecutiva, que ha cambiado y por que importa.",
        fuente="regular",
        tam=11,
        color=COLORES["gris_suave"],
    )
    p.y -= 38

    tarjetas = [
        ("01", "Mas orden", "La informacion ya no se mueve con la misma dependencia de archivos sueltos y correcciones manuales."),
        ("02", "Mas control", "Ahora es mucho mas facil saber que corrio, que quedo pendiente y donde aparecio un problema."),
        ("03", "Mas claridad", "Los casos especiales y los pendientes ya no se mezclan con errores generales del sistema."),
        ("04", "Mejor base", "La plataforma hoy ofrece una base mucho mas estable para seguimiento y proyecciones."),
    ]

    x_positions = [MARGEN_X, 305]
    y = p.y
    idx = 0
    for fila in range(2):
        for col in range(2):
            x = x_positions[col]
            titulo_num, titulo, cuerpo = tarjetas[idx]
            p.rectangulo(x, y - 130, 235, 118, color_relleno=COLORES["blanco"], color_trazo=(0.84, 0.84, 0.84), grosor=0.8)
            p.rectangulo(x, y - 22, 235, 22, color_relleno=COLORES["verde_claro"])
            p.texto(x + 12, y - 15, titulo_num, fuente="bold", tam=10, color=COLORES["bronce"])
            p.texto(x + 45, y - 15, titulo, fuente="bold", tam=12, color=COLORES["verde_oscuro"])
            yy = y - 42
            for linea in envolver(cuerpo, 34):
                p.texto(x + 12, yy, linea, fuente="regular", tam=10, color=COLORES["gris_texto"])
                yy -= 15
            idx += 1
        y -= 155


def renderizar_contenido() -> None:
    p = nueva_pagina()
    encabezado_contenido(p)
    p.y = ALTO - 110

    for tipo, valor in leer_bloques_md():
        if tipo == "blank":
            p.y -= 8
            continue

        if tipo == "h2":
            if p.necesita_espacio(42):
                p = nueva_pagina()
                encabezado_contenido(p)
                p.y = ALTO - 110
            p.rectangulo(MARGEN_X, p.y - 8, ANCHO - (MARGEN_X * 2), 26, color_relleno=COLORES["verde_oscuro"])
            p.texto(MARGEN_X + 12, p.y + 1, valor, fuente="bold", tam=13, color=COLORES["blanco"])
            p.y -= 34
            continue

        if tipo == "h3":
            if p.necesita_espacio(26):
                p = nueva_pagina()
                encabezado_contenido(p)
                p.y = ALTO - 110
            p.texto(MARGEN_X, p.y, valor, fuente="bold", tam=12, color=COLORES["bronce"])
            p.y -= 22
            continue

        if tipo == "li":
            lineas = envolver(valor, 78)
            alto = 18 * len(lineas) + 6
            if p.necesita_espacio(alto):
                p = nueva_pagina()
                encabezado_contenido(p)
                p.y = ALTO - 110
            p.rectangulo(MARGEN_X + 2, p.y - 1, 6, 6, color_relleno=COLORES["verde_acp"])
            p.texto(MARGEN_X + 18, p.y, lineas[0], fuente="regular", tam=11, color=COLORES["gris_texto"])
            p.y -= 16
            for extra in lineas[1:]:
                p.texto(MARGEN_X + 18, p.y, extra, fuente="regular", tam=11, color=COLORES["gris_texto"])
                p.y -= 16
            p.y -= 2
            continue

        if tipo == "p":
            lineas = envolver(valor, 88)
            alto = 16 * len(lineas) + 8
            if p.necesita_espacio(alto):
                p = nueva_pagina()
                encabezado_contenido(p)
                p.y = ALTO - 110
            for linea in lineas:
                p.texto(MARGEN_X, p.y, linea, fuente="regular", tam=11, color=COLORES["gris_texto"])
                p.y -= 16
            p.y -= 5


def construir_pdf() -> None:
    agregar_portada()
    agregar_pagina_mensajes()
    renderizar_contenido()

    for i, pagina in enumerate(paginas, start=1):
        if i > 1:
            pie_pagina(pagina, i)

    objetos: list[bytes | str | None] = []
    objetos.append("<< /Type /Catalog /Pages 2 0 R >>")
    objetos.append(None)
    objetos.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica >>")
    objetos.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Bold >>")
    objetos.append("<< /Type /Font /Subtype /Type1 /BaseFont /Helvetica-Oblique >>")

    page_refs: list[str] = []

    for pagina in paginas:
        stream = "\n".join(pagina.comandos).encode("cp1252", errors="replace")
        contenido = (
            b"<< /Length "
            + str(len(stream)).encode("ascii")
            + b" >>\nstream\n"
            + stream
            + b"\nendstream"
        )
        objetos.append(contenido)
        numero_contenido = len(objetos)
        page_obj = (
            f"<< /Type /Page /Parent 2 0 R /MediaBox [0 0 {ANCHO} {ALTO}] "
            f"/Resources << /Font << /F1 3 0 R /F2 4 0 R /F3 5 0 R >> >> "
            f"/Contents {numero_contenido} 0 R >>"
        )
        objetos.append(page_obj)
        page_refs.append(f"{len(objetos)} 0 R")

    objetos[1] = f"<< /Type /Pages /Kids [{' '.join(page_refs)}] /Count {len(page_refs)} >>"

    pdf = bytearray(b"%PDF-1.4\n")
    offsets = [0]
    for idx, obj in enumerate(objetos, start=1):
        offsets.append(len(pdf))
        pdf.extend(f"{idx} 0 obj\n".encode("ascii"))
        if isinstance(obj, bytes):
            pdf.extend(obj)
        else:
            pdf.extend(str(obj).encode("cp1252", errors="replace"))
        pdf.extend(b"\nendobj\n")

    xref = len(pdf)
    pdf.extend(f"xref\n0 {len(objetos)+1}\n".encode("ascii"))
    pdf.extend(b"0000000000 65535 f \n")
    for off in offsets[1:]:
        pdf.extend(f"{off:010d} 00000 n \n".encode("ascii"))
    pdf.extend(
        f"trailer\n<< /Size {len(objetos)+1} /Root 1 0 R >>\nstartxref\n{xref}\n%%EOF\n".encode(
            "ascii"
        )
    )

    DESTINO_PDF.write_bytes(pdf)


if __name__ == "__main__":
    construir_pdf()
    print(DESTINO_PDF)
