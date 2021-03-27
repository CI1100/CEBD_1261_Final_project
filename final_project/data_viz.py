import matplotlib.pyplot as plt


def get_timeline_fig(pdf, var_name, figsize=(12, 8)):
    fig, ax = plt.subplots(figsize=figsize)
    ax = plt.plot(pdf['date'], pdf[var_name])
    return fig
