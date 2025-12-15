To compile the report, just press the green triangle in the top right corner (or ctrl+alt+b). If the bibliography doesnt render, it is probably because i pasted some latex code from Overleaf into VSCode Latex Workshop. Then i should try adding this to the settings.json file:

"latex-workshop.latex.recipes": [
    {
        "name": "pdflatex -> biber -> pdflatex*2",
        "tools": [
            "pdflatex",
            "biber",
            "pdflatex",
            "pdflatex"
        ]
    }
]