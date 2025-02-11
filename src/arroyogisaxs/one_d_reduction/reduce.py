import numpy as np
import zmq

from .conversions import (
    get_azimuthal_integrator,
    pix_to_alpha_f,
    pix_to_theta_f,
    q_parallel,
    q_z,
)

host = "127.0.0.1"
port = "5001"

context = zmq.Context()
socket = context.socket(zmq.PUB)
socket.connect("tcp://{}:{}".format(host, port))


def pixel_roi_vertical_cut(
    masked_image,
    beamcenter_x,
    beamcenter_y,
    incident_angle,
    sample_detector_dist,
    wavelength,
    pix_size,
    cut_half_width,
    y_min,
    y_max,
    output_unit,
):
    shape = masked_image.shape

    y_min = max(0, y_min)
    y_max = min(shape[0], y_max + 1)

    x_min = max(0, int(beamcenter_x - cut_half_width))
    x_max = min(shape[1], int(beamcenter_x + cut_half_width + 1))

    cut_data = masked_image[
        y_min:y_max,
        x_min:x_max,
    ]

    cut_average = np.average(cut_data, axis=1)
    errors = np.sqrt(cut_average)

    pix = np.arange(y_min, y_max)
    if output_unit == "pixel":
        return (pix, cut_average, errors)
    else:
        # Set pixel coordinates in reference to beam center
        pix = pix - beamcenter_y
        af = pix_to_alpha_f(pix, sample_detector_dist, pix_size, incident_angle)
    if output_unit == "angle":
        return (af, cut_average, errors)
    elif output_unit == "q":
        qz = q_z(wavelength, af, incident_angle)
        return (qz, cut_average, errors)


def pixel_roi_horizontal_cut(
    masked_image,
    beamcenter_x,
    beamcenter_y,
    incident_angle,
    sample_detector_dist,
    wavelength,
    pix_size,
    cut_half_width,
    cut_pos_y,
    x_min,
    x_max,
    output_unit,
):
    """
    Extract a cut in horizontal direction on the detector with width=2*cut_half_width.
    """
    shape = masked_image.shape

    x_min = max(0, x_min)
    x_max = min(shape[1], x_max + 1)
    y_min = max(0, int(cut_pos_y - cut_half_width))
    y_max = min(shape[0], int(cut_pos_y + cut_half_width + 1))

    cut_data = masked_image[
        y_min:y_max,
        x_min:x_max,
    ]

    cut_average = np.average(cut_data, axis=0)
    errors = np.sqrt(cut_average)

    pix = np.arange(x_min, x_max)

    if output_unit == "pixel":
        return (pix, cut_average, errors)
    else:
        pix = pix - beamcenter_x
        af = pix_to_alpha_f(
            beamcenter_y - cut_pos_y,
            sample_detector_dist,
            pix_size,
            incident_angle,
        )
        tf = pix_to_theta_f(pix, sample_detector_dist, pix_size)
    if output_unit == "angle":
        return (tf, cut_average, errors)
    elif output_unit == "q":
        qp = q_parallel(wavelength, tf, af, incident_angle)
        return (qp, cut_average, errors)


def integrate1d_azimuthal(
    image,
    mask,
    beamcenter_x,
    beamcenter_y,
    sample_detector_dist,
    wavelength,
    pix_size,
    tilt,
    rotation,
    polarization_factor,
    num_bins,
    chi_min,
    chi_max,
    inner_radius,
    outer_radius,
    output_unit,  # Always q for now
):
    azimuthal_integrator = get_azimuthal_integrator(
        beamcenter_x,
        beamcenter_y,
        wavelength,
        sample_detector_dist,
        tilt,
        rotation,
        pix_size,
    )

    result = azimuthal_integrator.integrate1d(
        data=np.copy(image),
        mask=np.copy(mask),
        npt=num_bins,
        correctSolidAngle=True,
        error_model="poisson",
        # radial_range=(inner_radius, outer_radius),
        azimuth_range=(chi_min, chi_max),
        polarization_factor=polarization_factor,
    )

    return result


def integrate1d_radial(
    image,
    mask,
    beamcenter_x,
    beamcenter_y,
    sample_detector_dist,
    wavelength,
    pix_size,
    tilt,
    rotation,
    polarization_factor,
    num_bins,
    chi_min,
    chi_max,
    inner_radius,
    outer_radius,
    output_unit,  # Always q for now
):
    azimuthal_integrator = get_azimuthal_integrator(
        beamcenter_x,
        beamcenter_y,
        wavelength,
        sample_detector_dist,
        tilt,
        rotation,
        pix_size,
    )

    result = azimuthal_integrator.integrate_radial(
        # Copying here due to issue with memory ownership
        # line 323, in pyFAI.ext.splitBBoxCSR.CsrIntegrator.integrate_ng
        # "ValueError: buffer source array is read-only"
        data=np.copy(image),
        mask=mask,
        npt=num_bins,
        correctSolidAngle=True,
        # radial_range=(inner_radius, outer_radius),
        azimuth_range=(chi_min, chi_max),
        polarization_factor=polarization_factor,
    )

    return result


if __name__ == "__main__":
    parameters_azimuthal = {
        "input_uri_data": r"/raw/AgB_2024_03_25_10s_2m",
        "input_uri_mask": r"/processed/masks/AgB_2024_03_27_30s_lo_2m_mask/M_ROIMask",
        "beamcenter_x": 317.8,
        "beamcenter_y": 1245.28,
        "wavelength": 1.2398,
        "polarization_factor": 0.99,
        "sample_detector_dist": 274.83,
        "pix_size": 172,
        "chi_min": -180,
        "chi_max": 180,
        "inner_radius": 1,
        "outer_radius": 2900,
        "polarization_factor": 0.99,
        "rotation": 0.0,
        "tilt": 0.0,
        "num_bins": 800,
        "output_unit": "q",
    }
    # integrate1d_azimuthal_tiled(**parameters_azimuthal)
    parameters_horizontal = {
        "input_uri_data": "raw/ALS-S2VP42/218_A0p160_A0p160_sfloat_2m",
        "input_uri_mask": "processed/masks/ALS_BCP_Mixing_inverted",
        "beamcenter_x": 670.976,
        "beamcenter_y": 1180.42,
        "incident_angle": 0.16,
        "sample_detector_dist": 3513.21,
        "wavelength": 1.2398,
        "pix_size": 172,
        "cut_half_width": 10,
        "cut_pos_y": 1180 - 105,
        "x_min": 670 - 250,
        "x_max": 670 + 250,
        "output_unit": "q",
    }
