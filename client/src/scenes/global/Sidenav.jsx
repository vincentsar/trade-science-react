import KeyboardDoubleArrowRightIcon from "@mui/icons-material/KeyboardDoubleArrowRight";
import KeyboardDoubleArrowLeftIcon from "@mui/icons-material/KeyboardDoubleArrowLeft";
import { useState } from "react";
import PropTypes from "prop-types";
import { Sidebar, Menu, MenuItem, SubMenu } from "react-pro-sidebar";

export default function Sidenav({ data, onItemSelect }) {
  const [open, setopen] = useState(true);
  const toggleOpen = () => {
    setopen(!open);
  };

  return (
    <div style={{ display: "flex", height: "100vh" }}>
      <Sidebar className="app">
        <Menu>
          <MenuItem className="menu1">
            <h5>TRADE WITH SCIENCE</h5>
          </MenuItem>
          {Object.keys(data).map((key) => (
            <SubMenu key={key} label={key}>
              {data[key].map((item) => {
                return (
                  <MenuItem key={item} onClick={() => onItemSelect(item)}>
                    {item}
                  </MenuItem>
                );
              })}
            </SubMenu>
          ))}
        </Menu>
      </Sidebar>
    </div>
  );
}

Sidenav.propTypes = {
  data: PropTypes.objectOf(PropTypes.arrayOf(PropTypes.string)).isRequired,
  onItemSelect: PropTypes.func.isRequired,
};
