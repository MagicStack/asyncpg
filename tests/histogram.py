import py

from .compat import Iterable
from .utils import TIME_UNITS
from .utils import slugify

try:
    from pygal.graph.box import Box
    from pygal.style import DefaultStyle
except ImportError as exc:
    raise ImportError(exc.args, "Please install pygal and pygaljs or pytest-benchmark[histogram]")


class CustomBox(Box):
    def _box_points(self, serie, _):
        return serie, [serie[0], serie[6]]

    def _value_format(self, x):
        return "Min: {0[8]:.4f}\n" \
               "Q1-1.5IQR: {0[1]:.4f}\n" \
               "Q1: {0[2]:.4f}\nMedian: {0[3]:.4f}\nQ3: {0[4]:.4f}\n" \
               "Q3+1.5IQR: {0[5]:.4f}\n" \
               "Max: {0[9]:.4f}".format(x[:10])

    def _format(self, x, *args):
        sup = super(CustomBox, self)._format
        if args:
            val = x.values
        else:
            val = x
        if isinstance(val, Iterable):
            return self._value_format(val), val[7]
        else:
            return sup(x, *args)

    def _tooltip_data(self, node, value, x, y, classes=None, xlabel=None):
        super(CustomBox, self)._tooltip_data(node, value[0], x, y, classes=classes, xlabel=None)
        self.svg.node(node, 'desc', class_="x_label").text = value[1]


def make_plot(benchmarks, title, adjustment):
    class Style(DefaultStyle):
        guide_stroke_color = "rgba(0,0,0,0.4)"
        # colors = ["#000000" if row["path"] else DefaultStyle.colors[1]
        #          for row in benchmarks]
        colors = []
        for row in benchmarks:
            i = 1
            name = row["name"].replace("test_", "")
            if name == "min":
                i = 16
            elif name.startswith("record"):
                i = 4
            elif name.startswith("numpy"):
                i = 7
            if row["path"]:
                color = "#000000"
            else:
                color = DefaultStyle.colors[i]
            colors.append(color)

        font_family = 'Consolas, "Deja Vu Sans Mono", "Bitstream Vera Sans Mono", "Courier New", monospace'

    minimum = int(min(row["min"] * adjustment for row in benchmarks))
    maximum = int(max(
        min(row["max"], row["hd15iqr"]) * adjustment
        for row in benchmarks
    ) + 1)

    try:
        import pygaljs
    except ImportError:
        opts = {}
    else:
        opts = {
            "js": [
                pygaljs.uri("2.0.x", "pygal-tooltips.js")
            ]
        }
    x_labels = []
    for row in benchmarks:
        name = row["name"].replace("test_", "")
        if (param_pos := name.find("[")) >= 0:
            param = name[param_pos + 1:-1]
            name = name[:param_pos]
            label=f"{name}%{param}"
        else:
            label = name
        # <text x="([^"]+)"([^>]+)>([^<%]+)%([^<]+)
        # <text x="$1"$2><tspan x="$1" alignment-baseline="central">$3</tspan><tspan x="$1" alignment-baseline="central" dy="12">$4</tspan>
        x_labels.append(label)

    plot = CustomBox(
        box_mode="tukey",
        # x_label_rotation=-90,
        x_labels=x_labels,
        show_legend=False,
        title=title,
        x_title="Trial",
        y_title="Duration",
        style=Style,
        min_scale=20,
        max_scale=20,
        truncate_label=50,
        range=(0, maximum),
        zero=minimum,
        css=[
            "file://style.css",
            "file://graph.css",
            """inline:
                .tooltip .value {
                    font-size: 1em !important;
                }
                .axis text {
                    font-size: 9px !important;
                }
            """
        ],
        **opts
    )

    for row in benchmarks:
        serie = []
        for field in ["min", "ld15iqr", "q1", "median", "q3", "hd15iqr", "max"]:
            if field not in ("min", "max"):
                value = row[field] * adjustment
            else:
                value = -maximum / 40
            serie.append(value)
        serie.append(row["path"])
        for ex in ("min", "max"):
            serie.append(row[ex] * adjustment)
        plot.add("{0[fullname]} - {0[rounds]} rounds".format(row), serie)
    return plot


def make_histogram(output_prefix, name, benchmarks, unit, adjustment):
    if name:
        path = "{0}-{1}.svg".format(output_prefix, slugify(name))
        title = "Speed in {0} of {1}".format(TIME_UNITS[unit], name)
    else:
        path = "{0}.svg".format(output_prefix)
        title = "Speed in {0}".format(TIME_UNITS[unit])

    output_file = py.path.local(path).ensure()

    plot = make_plot(
        benchmarks=benchmarks,
        title=title,
        adjustment=adjustment,
    )
    plot.render_to_file(str(output_file))
    return output_file
